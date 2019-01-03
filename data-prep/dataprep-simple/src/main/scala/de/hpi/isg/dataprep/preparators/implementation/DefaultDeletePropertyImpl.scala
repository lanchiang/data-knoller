package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl

import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.model.error.{PreparationError, PropertyError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.define.DeleteProperty
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  *
  * @author Lan Jiang
  * @since 2018/8/20
  */
class DefaultDeletePropertyImpl extends AbstractPreparatorImpl {

  //    @throws(classOf[Exception])
  //    override protected def executePreparator(preparator: AbstractPreparator, dataFrame: Dataset[Row]): ExecutionContext = {
  //        val preparator_ = this.getPreparatorInstance(preparator, classOf[DeleteProperty])
  //        val errorAccumulator = this.createErrorAccumulator(dataFrame)
  //        executeLogic(preparator_, dataFrame, errorAccumulator)
  //    }

  //    private def executeLogic(preparator: DeleteProperty, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
  //        val targetPropertyName = preparator.propertyName
  //
  //        val columns = dataFrame.columns
  //
  //        val exist = columns.find(propertyName => propertyName.equals(targetPropertyName))
  //        val resultDataFrame = exist match {
  //            case Some(name) => {
  //                dataFrame.drop(dataFrame.col(targetPropertyName))
  //            }
  //            case None => {
  //                errorAccumulator.add(new PropertyError(targetPropertyName, new PreparationHasErrorException("The property to be deleted does not exist.")))
  //                dataFrame
  //            }
  //        }
  //
  //        resultDataFrame.persist()
  //
  //        new ExecutionContext(resultDataFrame, errorAccumulator)
  //    }

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[DeleteProperty]
    val targetPropertyName = preparator.propertyName

    val columns = dataFrame.columns

    val exist = columns.find(propertyName => propertyName.equals(targetPropertyName))
    val resultDataFrame = exist match {
      case Some(name) => {
        dataFrame.drop(dataFrame.col(targetPropertyName))
      }
      case None => {
        errorAccumulator.add(new PropertyError(targetPropertyName, new PreparationHasErrorException("The property to be deleted does not exist.")))
        dataFrame
      }
    }

    resultDataFrame.persist()

    new ExecutionContext(resultDataFrame, errorAccumulator)
  }
}
