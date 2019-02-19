package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.{MergeAttribute, MergeUtil}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, max, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

class DefaultMergeAttributeImpl extends  AbstractPreparatorImpl{

	override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
		val preparator = abstractPreparator.asInstanceOf[MergeAttribute];
		//find a name for the merged column
		val newColumnName = findName(preparator.attributes(0),preparator.attributes(1))

		//find conflict resolution function
		val conflictFunc = if (preparator.handleMergeConflict != null) preparator.handleMergeConflict else MergeUtil.handleMergeConflicts _
		//select the right merge function
		val mergeFunc = if (MergeUtil.isNull(preparator.connector)) merge(conflictFunc) else merge(preparator.connector)

		//merge
		val df = dataFrame.withColumn(newColumnName, mergeFunc(col(preparator.attributes(0)),col(preparator.attributes(1))))
		//delete old columns
		val deleteColumns = preparator.attributes.splitAt(2)._1.filter(x => x!= newColumnName)
		val columns = df.columns.filter( c => !deleteColumns.contains(c)).map(col(_))

		val result = df.select(columns: _*)
		if(preparator.attributes.length>2)
		{
			preparator.attributes = newColumnName :: preparator.attributes.splitAt(2)._2
			executeLogic(preparator,result,errorAccumulator)
		}
		else new ExecutionContext(result,errorAccumulator)
	}

	def merge(connector: String): UserDefinedFunction =
		udf((col1: String,col2: String) => {
			if(MergeUtil.isNull(col1) && MergeUtil.isNull(col2))
				null
			else if(MergeUtil.isNull(col1))
				col2
			else if (MergeUtil.isNull(col2))
				col1
			else
				col1 + connector + col2
	})

	def merge(handleConflicts:(String,String)=>String):UserDefinedFunction =
	udf((col1: String,col2: String) => {
		if (MergeUtil.isNull(col1)) col2 else if (MergeUtil.isNull(col2)) col1 else if (col1.equals(col2)) col1  else handleConflicts(col1,col2)
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
