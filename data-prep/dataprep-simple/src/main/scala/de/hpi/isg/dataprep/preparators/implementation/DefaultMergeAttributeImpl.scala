package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.{ExecutionContext, StringUtils}
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.{MergeAttribute, MergePropertiesUtil}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

class DefaultMergeAttributeImpl extends AbstractPreparatorImpl {

	/**
	  * Recursively merges columns. Selects column-names, merge function, conflict resolution function.
		*
	  * @param abstractPreparator is the instance of {@link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
	  * @param dataFrame          contains the intermediate dataset
	  * @param errorAccumulator   is the {@link CollectionAccumulator} to store preparation errors while executing the preparator.
	  * @return an instance of {@link ExecutionContext} that includes the new dataset, and produced errors.
	  */
	override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
		val preparator = abstractPreparator.asInstanceOf[MergeAttribute]

		//find a name for the merge result column
		val newColumnName = findName(preparator.attributes(0),preparator.attributes(1))

		//find conflict resolution function
		val conflictFunc = preparator.handleMergeConflict != null match {
			case true => preparator.handleMergeConflict
			case false => MergePropertiesUtil.handleMergeConflicts _
		}

		//select the right merge function
		val mergeFunc = MergePropertiesUtil.isNull(preparator.connector) match {
			case true => merge(conflictFunc)
			case false => merge(preparator.connector)
		}

		//merge
		val df = dataFrame.withColumn(newColumnName, mergeFunc(col(preparator.attributes(0)),col(preparator.attributes(1))))

		//only remove merged columns (we only looked at the first 2 in the list)
		//make a list of columns that should be deleted
		val result = df

		//merge recursively if more than 2 columns are given
		preparator.attributes.length > 2 match {
			case true => {
				preparator.attributes = newColumnName :: preparator.attributes.splitAt(2)._2
				executeLogic(preparator, result, errorAccumulator)
			}
			case false => new ExecutionContext(result, errorAccumulator)
		}
	}

	/**
	  * This spark UDF merges two given columns by concatenating them.
	  * If it encounters null values it only returns the other value.
	  * If both values are null it returns null.
		*
	  * @param connector string to connect two values
	  * @return concatenation of given columns with connector col1 + connector + col2
	  */
	def merge(connector: String): UserDefinedFunction = {
		udf((col1: String, col2: String) => MergePropertiesUtil.merge(col1, col2, connector))
	}

	/**
	  * This spark UDF merges two given columns by selecting one of the values.
	  * Values are merged if they are equal or one of them is null.
	  * A merge conflict exists when the values are not equal.
	  * To resolve the merge conflict the handleConflicts function is called.
	  * The handleConflicts function can be defined when creating the preparator.
	  * If not handleConflicts function is specified, the default implementation MergeUtil.handleMergeConflicts is chosen.
		*
	  * @param handleConflicts the handleConflicts function is called to resolve merge conflicts
	  * @return merged column
	  */
	def merge(handleConflicts: (String,String) => String): UserDefinedFunction = {
		udf((col1: String,col2: String) => MergePropertiesUtil.merge(col1,col2,handleConflicts))
	}

	/**
	  * findName finds a name for the new column that contains the merge result.
	  * @param a name of the fist input column
	  * @param b name of the second input column
	  * @return name of the resulting merged column
	  */
	def findName(a:String, b:String) = {
		val lcs =  StringUtils.longestCommonSubstring(a,b)
		//TODO relative on string length?
		if (lcs.length > 1) lcs else a
	}

	override def findMissingParametersImpl(preparator: AbstractPreparator): Unit = {
		// Todo: 1) getMissingMandatoryParameters
		// Todo: 2) call the discovery functions for each of the missing parameters
		// Todo: 3) set the discovered values to these parameters.

		???
	}
}
