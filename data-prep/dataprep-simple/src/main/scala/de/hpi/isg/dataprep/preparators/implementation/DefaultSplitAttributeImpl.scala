package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitAttribute
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.sql.functions.{col, udf}

// TODO prerequisite property to split should be a string
class DefaultSplitAttributeImpl extends PreparatorImpl {
  /**
    * The abstract class of preparator implementation.
    *
    * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
    * @param dataFrame          contains the intermediate dataset
    * @param errorAccumulator   is the { @link CollectionAccumulator} to store preparation errors while executing the preparator.
    * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
    * @throws Exception
    */
  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: DataFrame,
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[SplitAttribute]
    val propertyName = preparator.propertyName
    val separator = preparator.separator
    val times = preparator.times
    val startLeft = preparator.startLeft

    (separator, startLeft, times) match {
      case (null, startLeft, -1) => split(dataFrame, errorAccumulator, propertyName)
      case (separator, startLeft, -1) => split(dataFrame, errorAccumulator, propertyName, separator)
      case (separator, startLeft, times) => split(dataFrame, errorAccumulator, propertyName, separator, startLeft, times)
    }
  }

  def split(dataFrame: DataFrame, errorAccumulator: CollectionAccumulator[PreparationError], propertyName: String, separator: String, startLeft: Boolean, times: Int): ExecutionContext = ???

  def split(dataFrame: DataFrame, errorAccumulator: CollectionAccumulator[PreparationError], propertyName: String, separator: String): ExecutionContext = {
    val newDf = dataFrame.withColumn("newCol", split(separator)(col(propertyName)))
    new ExecutionContext(newDf, errorAccumulator)
  }


  def split(dataFrame: DataFrame, errorAccumulator: CollectionAccumulator[PreparationError], propertyName: String): ExecutionContext = ???

  def split(separator: String, times: Option[Int] = None, startLeft: Boolean = true): UserDefinedFunction =
    udf((s: String) => {
      val splitted = if (startLeft) s.split(separator) else s.reverse.split(separator).reverse
      if (times.isEmpty) splitted else splitted.take(times.get)
    })


}
