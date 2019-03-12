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

	/** *  THRESHOLDS FOR calApplicability  ***/
	val threshold = 0.95


	/** * CTORS ***/
	def this() {
		this(List[String](), "")
	}

	def this(attributes: java.util.List[String], connector: String) {
		this((attributes.asScala.toList), connector, null)
	}


	//def mapMerge(row: Row) = MergeUtil.isGoodToMerge(row.getString(0),row.getString(1))

	/** * DATE HANDLING ***/

	/** *
	  * Checks if header to find year column.
	  *
	  * @param header column name
	  * @return true if column is year
	  */
	def headerIsYear(header: String): Boolean = if (header.toLowerCase.contains("year")) true else false

	/** *
	  * Checks if header to find month column.
	  *
	  * @param header column name
	  * @return true if column is month
	  */
	def headerIsMonth(header: String): Boolean = if (header.toLowerCase.contains("month")) true else false

	/** *
	  * Checks if header to find day column.
	  *
	  * @param header column name
	  * @return true if column is day
	  */
	def headerIsDay(header: String): Boolean = if (header.toLowerCase.contains("day")) true else false

	/** *
	  * Check column contents to check if it contains the year part of a date.
	  *
	  * @param dataset    dataset to find year in
	  * @param columnName name of column to inspect
	  * @return true if column is year
	  */
	def isYear(dataset: Dataset[Row], columnName: String): Boolean = {
		import dataset.sparkSession.implicits._

		val columnWithoutNull = getColumnsWithoutNull(dataset, columnName)

		try {

			val sumYear = columnWithoutNull
					.map(x => if (x <= 2100 && x > 0) 1 else 0)
					.reduce(_ + _)
		if (sumYear / columnWithoutNull.count() >= threshold)
			true
		else
			false
		} catch {
			case _: Throwable => return false
		}

	}

	/** *
	  * Check column contents to check if it contains the month part of a date.
	  *
	  * @param dataset    dataset to find month in
	  * @param columnName name of column to inspect
	  * @return true if column is month
	  */
	def isMonth(dataset: Dataset[Row], columnName: String): Boolean = {
		import dataset.sparkSession.implicits._

		val columnWithoutNull = getColumnsWithoutNull(dataset, columnName)

		try {
			val sumMonth = columnWithoutNull
					.map(x => if (x <= 12 && x > 0) 1 else 0)
					.reduce(_ + _)
		if (sumMonth / columnWithoutNull.count() >= threshold)
			true
		else
			false
		} catch {
			case _: Throwable => return false
		}

	}

	/** *
	  * Check column contents to check if it contains the day part of a date.
	  *
	  * @param dataset    dataset to find day in
	  * @param columnName name of column to inspect
	  * @return true if column is day
	  */
	def isDay(dataset: Dataset[Row], columnName: String): Boolean = {
		import dataset.sparkSession.implicits._

		val columnWithoutNull = getColumnsWithoutNull(dataset, columnName)
		try {
			val sumDay = columnWithoutNull
					.map(x => if (x <= 31 && x > 0) 1 else 0)
					.reduce(_ + _)
		if (sumDay / columnWithoutNull.count() >= threshold)
			true
		else
			false
		} catch {
			case _: Throwable => return false
		}

	}

	/***
	  * Return a dataset with only the given column, all null rows removed and converted to int.
	  * @param dataset input dataset
	  * @param columnName name of the column to keep
	  * @return	dataset[Int] none null rows in the given collumn
	  */
	def getColumnsWithoutNull(dataset: Dataset[Row], columnName: String):Dataset[Int] = {
		import dataset.sparkSession.implicits._
		dataset
				.select(columnName)
				.filter(x => {
					val str = if (x.isNullAt(0)) null else x.get(0).toString
					!MergeUtil.isNull(str)
				})
				.map(x => x.getString(0).toInt)
	}

	override def buildMetadataSetup(): Unit = {

	}

	/** *
	  * This function calculates the callApplicability assuming we want to merge date parts (year, month, day).
	  * First the column names are checked, if no match was found, the rows are checked to be in value ranges, that
	  * would make sense for dates.
	  *
	  * @param schemaMapping  is the schema of the input data towards the schema of the output data.
	  * @param dataset        is the input dataset slice. A slice can be a subset of the columns of the data,
	  *                       or a subset of the rows of the data.
	  * @param targetMetadata is the set of { @link Metadata} that shall be fulfilled for the output data
	  * @return 0 if list of rows is not (day, month, year) or more or less than 3 columns are given. returns 1 if date if found
	  */
	def callApplicabilityDate(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
		import dataset.sparkSession.implicits._
		val columns = dataset.columns.toSeq
		if (columns.length != 3)
			return 0

		this.attributes = columns.toList.reverse
		this.connector = "."

		//check headers
		if (headerIsDay(columns(2)) && headerIsMonth(columns(1)) && headerIsYear(columns(0))) {
			return 1
		}
		//check rows
		else if (isDay(dataset, columns(2)) && isMonth(dataset, columns(1)) && isYear(dataset, columns(0))) {
			return 1
		}
		else
			return 0
	}

	/** *
	  * Calculates calApplicability for columns to merge.
	  * Expects 2 columns for normal merge and 3 columns for date-parts-merge.
	  * If more than 3 columns are given, returns 0.
	  *
	  * @param schemaMapping  is the schema of the input data towards the schema of the output data.
	  * @param dataset        is the input dataset slice. A slice can be a subset of the columns of the data,
	  *                       or a subset of the rows of the data.
	  * @param targetMetadata is the set of { @link Metadata} that shall be fulfilled for the output data
	  **/
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
		val result = if (applicability < threshold ) 0 else applicability
		//remember columns for later merge operation
		this.attributes = columns.toList
		//		chi2Test(dataset)
		//return weighted mergeGoodness
		result
	}
}

/** *
  * This object implements some helper methods used for merging.
  */
object MergeUtil extends Serializable {

	/** *
	  * Checks for null values in string. eg. null (nullptr), "null"
	  *
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

	/** *
	  * This function calculates if string a and b can be merged, without causing a conflict.
	  *
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

	/** *
	  * Default implementation of merge conflict handler.
	  * This function is called, when two values can not be merged.
	  * To resolve the conflict, the two values are concatenated with a whitespace between them.
	  *
	  * @param a string to merge
	  * @param b an other string to merge
	  * @return merged string of a and b
	  */
	def handleMergeConflicts(a: String, b: String) = {
		merge(a, b, " ")
	}

	/***
	  * Merges two strings and use handleConflicts - function to handle mergeconflicts.
	  * @param col1 String to merge
	  * @param col2	2nd String to merge
	  * @param handleConflicts function to handle merge-conflicts
	  * @return merged string
	  */
	def merge(col1: String, col2: String, handleConflicts: (String, String) => String): String = {
		if (MergeUtil.isNull(col1))
			col2
		else if (MergeUtil.isNull(col2))
			col1
		else if (col1.equals(col2))
			col1
		else
			handleConflicts(col1, col2)
	}

	/***
	  * Merge two strings by concatenation.
	  * @param col1 first String
	  * @param col2 second String
	  * @param connector connector between Strings
	  * @return	null if both strings are null, or one string only if the other is null, else col1 + connector + col2
	  */
	def merge(col1: String, col2: String, connector: String): String = {
		if (MergeUtil.isNull(col1) && MergeUtil.isNull(col2))
			null
		else if (MergeUtil.isNull(col1))
			col2
		else if (MergeUtil.isNull(col2))
			col1
		else
			col1 + connector + col2
	}
}
