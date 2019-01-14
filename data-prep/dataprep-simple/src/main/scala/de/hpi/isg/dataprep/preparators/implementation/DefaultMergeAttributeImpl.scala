package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.MergeAttribute
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf, max}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

class DefaultMergeAttributeImpl extends  AbstractPreparatorImpl{
	/**
	  * The abstract class of preparator implementation.
	  *
	  * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
	  * @param dataFrame        contains the intermediate dataset
	  * @param errorAccumulator is the { @link CollectionAccumulator} to store preparation errors while executing the preparator.
	  * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
	  * @throws Exception
	  */
	override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
		val preparator = abstractPreparator.asInstanceOf[MergeAttribute];
		val newColumnName = findName(preparator.attributes(0),preparator.attributes(1))
		val mergeFunc = if (preparator.connector.isEmpty) merge else merge(preparator.connector)
		val df = dataFrame.withColumn(newColumnName, mergeFunc(col(preparator.attributes(0)),col(preparator.attributes(1))))
		val deleteColumns = preparator.attributes.filter(x => x!= newColumnName)
		val columns = df.columns.filter( c => !deleteColumns.contains(c)).map(col(_))
		//findMergeCandidates(dataFrame)
		new ExecutionContext(df.select(columns: _*),errorAccumulator)
	}
	def merge(connector: String): UserDefinedFunction =
		udf((col1: String,col2: String) => {
			col1 + connector + col2
	})

	def findMergeCandidates( dataset: Dataset[Row]) =
	{
		import dataset.sparkSession.implicits._
		val columns = dataset.columns.toSeq

		columns.flatMap (
			col => (
					columns.filter(x => x != col).map(x=>(col,x))
					)
		)
		.map(x => dataset.select(x._1, x._2))
		.map(colCombo =>
					colCombo.map(
						row =>{
								val a = row.getString(0)
								val b = row.getString(1)
								if (a.equals(b))
									1
								else if ( a.trim.isEmpty || b.trim.isEmpty)
									1
								else
									0
					}
					).reduce(_ + _)
			).toDF().agg(col("__1"))

	}
	def isNull(value:String) = value.trim.isEmpty
	def isGoodToMerge(a:String, b:String):Integer = {
		if (a.equals(b))
			1
		else if ( isNull(a) || isNull(b))
			1
		else
			0
	}

	def merge():UserDefinedFunction =
	udf((col1: String,col2: String) => {
		if (col1.equals(col2)) col1 else if (col2.trim.nonEmpty) col2 else col1
	})

	def getAllSubstrings(str: String): Set[String] = {
		str.inits.flatMap(_.tails).toSet
	}
	def longestCommonSubstring(str1: String, str2: String): String = {
		val str1Substrings = getAllSubstrings(str1)
		val str2Substrings = getAllSubstrings(str2)

		str1Substrings.intersect(str2Substrings).maxBy(_.length)
	}

	def findName(a:String, b:String) = {
		val lcs =  longestCommonSubstring(a,b)
		//TODO relative on string length?
		if (lcs.length > 1) lcs else a
	}
}
