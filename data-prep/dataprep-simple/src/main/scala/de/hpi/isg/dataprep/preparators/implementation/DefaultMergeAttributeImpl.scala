package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.MergeAttribute
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
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
		val df = dataFrame.withColumn("__merged", merge(preparator.connector)(col(preparator.attributes(0)),col(preparator.attributes(1))))
		new ExecutionContext(df,errorAccumulator)
	}
	def merge(connector: String): UserDefinedFunction =
		udf((col1: String,col2: String) => {
			col1 + connector + col2
	})
}
