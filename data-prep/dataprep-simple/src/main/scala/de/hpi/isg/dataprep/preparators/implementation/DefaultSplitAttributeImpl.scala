package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitAttribute
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

class DefaultSplitAttributeImpl extends PreparatorImpl {
  /**
    * The abstract class of preparator implementation.
    *
    * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
    * @param dataFrame        contains the intermediate dataset
    * @param errorAccumulator is the { @link CollectionAccumulator} to store preparation errors while executing the preparator.
    * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
    * @throws Exception
    */
  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row],
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[SplitAttribute]
    val propertyName = preparator.propertyName
    val separator = preparator.separator
    val times = preparator.times
    val startLeft = preparator.startLeft

    (separator, startLeft, times) match {
      case (null, startLeft, -1) => split(propertyName)
      case (separator, startLeft, -1) => split(propertyName, separator)
      case (separator, startLeft, times) => split(propertyName, separator, startLeft, times)
    }
  }

  def split(propertyName: String, separator: String, startLeft: Boolean, times: Int): ExecutionContext = ???
  def split(propertyName: String, separator: String): ExecutionContext = ???
  def split(propertyName: String): ExecutionContext = ???
}
