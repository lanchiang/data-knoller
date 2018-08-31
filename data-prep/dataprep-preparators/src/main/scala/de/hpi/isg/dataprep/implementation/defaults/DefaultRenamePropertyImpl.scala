package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.Consequences
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.implementation.RenamePropertyImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, PropertyError}
import de.hpi.isg.dataprep.model.target.preparator.{Preparator, PreparatorImpl}
import de.hpi.isg.dataprep.preparators.RenameProperty
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  * @author Lan Jiang
  * @since 2018/8/19
  */
class DefaultRenamePropertyImpl extends PreparatorImpl {

    @throws(classOf[Exception])
    override protected def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        val preparator_ = this.getPreparatorInstance(preparator, classOf[RenameProperty])
        val errorAccumulator = this.createErrorAccumulator(dataFrame)
        executeLogic(preparator_, dataFrame, errorAccumulator)
    }

    private def executeLogic(preparator: RenameProperty, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val targetPropertyName = preparator.getNewPropertyName
        val currentPropertyName = preparator.getPropertyName
        val columns = dataFrame.columns

        var resultDataFrame = dataFrame

        // needs to check whether the new name string is valid.
        if (!columns.contains(currentPropertyName)) {
            errorAccumulator.add(new PropertyError(currentPropertyName, new PreparationHasErrorException("Property name does not exist.")));
        } else {
            if (columns.contains(targetPropertyName)) {
                errorAccumulator.add(new PropertyError(targetPropertyName, new PreparationHasErrorException("New name already exists.")));
            } else {
                resultDataFrame = dataFrame.withColumnRenamed(currentPropertyName, targetPropertyName)
            }
        }

        new Consequences(resultDataFrame, errorAccumulator)
    }
}
