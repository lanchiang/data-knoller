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

	override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
		val preparator = abstractPreparator.asInstanceOf[MergeAttribute];
		//find a name for the merged column
		val newColumnName = findName(preparator.attributes(0),preparator.attributes(1))
		//select the right merge function
		val mergeFunc = if (preparator.mergeDate) mergeDate else if (preparator.connector.isEmpty) merge else merge(preparator.connector)
		//merge
		val df = dataFrame.withColumn(newColumnName, mergeFunc(col(preparator.attributes(0)),col(preparator.attributes(1))))
		//delete old columns
		val deleteColumns = preparator.attributes.filter(x => x!= newColumnName)
		val columns = df.columns.filter( c => !deleteColumns.contains(c)).map(col(_))

		new ExecutionContext(df.select(columns: _*),errorAccumulator)
	}

	def mergeDate(): UserDefinedFunction =
		udf((col1: String,col2: String,col3: String) => {
			col1 + "." + col2 + "." +col3
		})

	def merge(connector: String): UserDefinedFunction =
		udf((col1: String,col2: String) => {
			col1 + connector + col2
	})

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
