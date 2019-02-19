package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import org.apache.spark.sql.{Dataset, Row, functions}
import org.apache.spark.sql.functions.udf
import spire.std.float

import scala.collection.JavaConverters._

class MergeAttribute(var attributes: List[String]
					 , var connector: String
					 , val handleMergeConflict: (String, String) => String = null)
		extends AbstractPreparator {

	/***  THRESHOLDS FOR calApplicability  ***/
	val weight = 4
	val bias = -0.75



	/** * CTORS ***/
	def this() {
		this(List[String](), "")
	}

	def this(attributes: java.util.List[String], connector: String) {
		this((attributes.asScala.toList), connector, null)
	}


	//def mapMerge(row: Row) = MergeUtil.isGoodToMerge(row.getString(0),row.getString(1))

	/** * DATE HANDLING ***/

	/***
	  * Checks if header to find year column.
	  * @param header column name
	  * @return true if column is year
	  */
	def headerIsYear(header: String): Boolean = if (header.toLowerCase.contains("year")) true else false
	/***
	  * Checks if header to find month column.
	  * @param header column name
	  * @return true if column is month
	  */
	def headerIsMonth(header: String): Boolean = if (header.toLowerCase.contains("month")) true else false
	/***
	  * Checks if header to find day column.
	  * @param header column name
	  * @return true if column is day
	  */
	def headerIsDay(header: String): Boolean = if (header.toLowerCase.contains("day")) true else false

	/***
	  * Check column contents to check if it contains the year part of a date.
	  * @param dataset dataset to find year in
	  * @param header name of column to inspect
	  * @return true if column is year
	  */
	def isYear(dataset: Dataset[Row], header: String): Boolean = {
		import dataset.sparkSession.implicits._

		val columnWithoutNull = dataset
				.select(header)
				.filter(x => MergeUtil.isNull(x.get(0).toString))

		val sumYear = columnWithoutNull
				.map(x => if (x.getInt(0) <= 2100 && x.getInt(0) > 0) 1 else 0)
				.reduce(_ + _)

		if (sumYear / columnWithoutNull.count() >= 1)
			true
		else
			false
	}

	/***
	  * Check column contents to check if it contains the month part of a date.
	  * @param dataset dataset to find month in
	  * @param header name of column to inspect
	  * @return true if column is month
	  */
	def isMonth(dataset: Dataset[Row], header: String): Boolean = {
		import dataset.sparkSession.implicits._

		val columnWithoutNull = dataset
				.select(header)
				.filter(x => x.get(0).toString.trim.isEmpty)

		val sumMonth = columnWithoutNull
				.map(x => if (x.getInt(0) <= 12 && x.getInt(0) > 0) 1 else 0)
				.reduce(_ + _)

		if (sumMonth / columnWithoutNull.count() >= 0.95)
			true
		else
			false
	}

	/***
	  * Check column contents to check if it contains the day part of a date.
	  * @param dataset dataset to find day in
	  * @param header name of column to inspect
	  * @return true if column is day
	  */
	def isDay(dataset: Dataset[Row], header: String): Boolean = {
		import dataset.sparkSession.implicits._

		val columnWithoutNull = dataset
				.select(header)
				.filter(x => x.get(0).toString.trim.isEmpty)

		val sumDay = columnWithoutNull
				.map(x => if (x.getInt(0) <= 31 && x.getInt(0) > 0) 1 else 0)
				.reduce(_ + _)

		if (sumDay / columnWithoutNull.count() >= 0.95)
			true
		else
			false
	}
	
	override def buildMetadataSetup(): Unit = {

	}

	/***
	  * This function calculates the callApplicability assuming we want to merge date parts (year, month, day).
	  *
	  * @param schemaMapping is the schema of the input data towards the schema of the output data.
	  * @param dataset is the input dataset slice. A slice can be a subset of the columns of the data,
	  *                or a subset of the rows of the data.
	  * @param targetMetadata is the set of {@link Metadata} that shall be fulfilled for the output data
	  * @return 0 if list of rows is not (day, month, year) or more or less than 3 columns are given. returns 1 if date if found
	  */
	def callApplicabilityDate(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    import dataset.sparkSession.implicits._
    val columns = dataset.columns.toSeq
    this.attributes = columns.toList
    this.connector = "."
    if (columns.length != 3)
      return 0
    if (headerIsDay(columns(0)) && headerIsMonth(columns(1)) && headerIsYear(columns(2))) {
      return 1
    }
    else if (isDay(dataset, columns(0)) && isMonth(dataset, columns(1)) && isYear(dataset, columns(2))) {
      return 1
    }
    else
      return 0
  }
	/***
	  * Calculates calApplicability for columns to merge.
	  * Expects 2 columns for normal merge and 3 columns for date-parts-merge.
	  * If more than 3 columns are given, returns 0.
	  * @param schemaMapping is the schema of the input data towards the schema of the output data.
	  * @param dataset is the input dataset slice. A slice can be a subset of the columns of the data,
	  *                or a subset of the rows of the data.
	  * @param targetMetadata is the set of {@link Metadata} that shall be fulfilled for the output data
	  * 	  */
	override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
		import dataset.sparkSession.implicits._
		val columns = dataset.columns.toSeq
		if (columns.length == 3)
			return callApplicabilityDate(schemaMapping, dataset, targetMetadata)
		if (columns.length != 2)
			return 0

		val applicability = dataset
				.map(x => {
					val a = if (x.isNullAt(0)) null else x.get(0).toString
					val b = if (x.isNullAt(1)) null else x.get(1).toString
					MergeUtil.isGoodToMerge(a, b) //calculate mergeGoodness for each row
				})
				.reduce(_ + _) / dataset.count().toFloat
		//apply threshold
		val result = (Math.max(applicability + bias, 0) * weight).toFloat
		//remember columns for later merge operation
		this.attributes = columns.toList
		//		chi2Test(dataset)
		//return weighted mergeGoodness
		result
	}

	def chi2Test(dataset: Dataset[Row]) = {
		import dataset.sparkSession.implicits._
		//	if dataset.columns.length != 2 throw Exception("Bla")
		//		val colA = dataset.select(dataset.columns(0))
		//		val colB = dataset.select(dataset.columns(1))
		//		val nA = colA.groupBy(colA.columns(0)).count()
		//		val nB = colB.groupBy(colB.columns(0)).count()

		dataset
				.groupBy(dataset.columns(0), dataset.columns(1))
				.count()
				.show()

		//		val countA = nA.map(_.getInt(0)).reduce(_+_)
		//		val countB = nB.map(_.getInt(0)).reduce(_+_)
		//
		//		val n = countA + countB
		//
		//		val Ea = nA
		//        		.map(x => countA * x.getInt(0) / n)
		//		val Eb = nB
		//				.map(x => countB * x.getInt(0) / n)
		//
		//		val chi2 = nA.

	}
}

/***
  * This object implements some helper methods used for merging.
  */
object MergeUtil extends Serializable {

	/***
	  * Checks for null values in string. eg. null (nullptr), "null"
	  * @param value string to check for null
	  * @return true if string is a null value
	  */
	def isNull(value: String): Boolean = {
		if (value == null)
			true
		else if (value.toLowerCase == "null")
			true
		else
			value.trim.isEmpty
	}

	/***
	  * This function calculates if string a and b can be merged, without causing a conflict.
	  * @param a string to merge
	  * @param b an other string to merge
	  * @return true if merge would not generate a conflict
	  */
	def isGoodToMerge(a: String, b: String): Integer = {
		if (isNull(a) || isNull(b))
			1
		else if (a.equals(b))
			1
		else
			0
	}

	/***
	  * Default implementation of merge conflict handler.
	  * This function is called, when two values can not be merged.
	  * To resolve the conflict, the two values are concatenated with a whitespace between them.
	  * @param a string to merge
	  * @param b an other string to merge
	  * @return merged string of a and b
	  */
	def handleMergeConflicts(a: String, b: String) = {
		a + " " + b
	}
}
