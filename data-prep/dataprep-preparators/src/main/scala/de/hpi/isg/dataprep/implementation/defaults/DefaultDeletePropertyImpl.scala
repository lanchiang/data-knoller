package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.Consequences
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.implementation.DeletePropertyImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, PropertyError}
import de.hpi.isg.dataprep.model.target.preparator.{Preparator, PreparatorImpl}
import de.hpi.isg.dataprep.preparators.DeleteProperty
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  *
  * @author Lan Jiang
  * @since 2018/8/20
  */
class DefaultDeletePropertyImpl extends PreparatorImpl {

    @throws(classOf[Exception])
    override protected def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        val preparator_ = this.getPreparatorInstance(preparator, classOf[DeleteProperty])
        val errorAccumulator = this.createErrorAccumulator(dataFrame)
        executeLogic(preparator_, dataFrame, errorAccumulator)
    }

    private def executeLogic(preparator: DeleteProperty, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val targetPropertyName = preparator.getPropertyName

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

        new Consequences(resultDataFrame, errorAccumulator)
    }
}
