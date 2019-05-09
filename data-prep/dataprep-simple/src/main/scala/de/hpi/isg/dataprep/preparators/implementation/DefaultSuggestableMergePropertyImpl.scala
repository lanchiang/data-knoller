package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.{MergePropertiesUtil, SuggestableMergeProperty}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  * @author Lan Jiang
  * @since 2019-04-17
  */
class DefaultSuggestableMergePropertyImpl extends AbstractPreparatorImpl {

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row],
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    // check whether necessary parameters are already set. Derive the missing ones by analyzing the dataset
    findMissingParametersImpl(abstractPreparator, dataFrame)
    val preparator = abstractPreparator.asInstanceOf[SuggestableMergeProperty]

    //select the right merge function
    val mergeFunc = merge(preparator.connector)

    val newColumnName = findName(dataFrame)

    //merge
    val result = dataFrame.withColumn(newColumnName, mergeFunc(col(preparator.attributes(0)), col(preparator.attributes(1))))

    //only remove merged columns (we only looked at the first 2 in the list)
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
    * Finds a name for the new column that contains the merge result.
    * @return name of the merge result column
    */
  def findName(dataset: DataFrame) : String = {
    ???
  }

  override def findMissingParametersImpl(preparator: AbstractPreparator, dataFrame: Dataset[Row]): Unit = {
    val cast = preparator match {
      case _ : SuggestableMergeProperty => preparator.asInstanceOf[SuggestableMergeProperty]
      case _ => throw new RuntimeException(new ClassCastException("Preparator class cannot be cast."))
    }

    val attributes = cast.attributes
    attributes match {
      case Nil => throw new RuntimeException(new ParameterNotSpecifiedException("Properties to merge are not specified."))
      case _ => {
        // properties are set correctly.
        cast.connector = cast.connector match {
          case null => findConnector(dataFrame)
          case _ => cast.connector
        }
      }
    }
  }

  /**
    * Find the proper connector character to merge the properties by analyzing the dataset.
    *
    * @param dataset is the dataset to be analyzed.
    * @return the discovered connector.
    */
  def findConnector(dataset: DataFrame): String = {
    val connector = DefaultSuggestableMergePropertyImpl.DEFAULT_CONNECTOR

    // Todo: here find the connector.

    connector
  }
}

object DefaultSuggestableMergePropertyImpl {

  private val DEFAULT_NEW_COLUMN_NAME = "newCol"

  private val DEFAULT_CONNECTOR = "|"
}