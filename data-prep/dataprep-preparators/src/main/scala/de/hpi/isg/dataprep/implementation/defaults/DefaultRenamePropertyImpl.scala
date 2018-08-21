package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.Consequences
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.implementation.RenamePropertyImpl
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  * @author Lan Jiang
  * @since 2018/8/19
  */
class DefaultRenamePropertyImpl extends RenamePropertyImpl {

    override def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        val errorAccumulator = new CollectionAccumulator[(Any, Throwable)]
        dataFrame.sparkSession.sparkContext.register(errorAccumulator, "The error accumulator for preparator: Rename property.")

        val preparator_ = super.getPreparatorInstance(preparator)

        val targetPropertyName = preparator_.getNewPropertyName
        val currentPropertyName = preparator_.getPropertyName
        val columns = dataFrame.columns

        var resultDataFrame = dataFrame

        // needs to check whether the new name string is valid.
        if (!columns.contains(currentPropertyName)) {
            errorAccumulator.add(currentPropertyName, new PreparationHasErrorException("Property name does not exist."))
        } else {
            if (columns.contains(targetPropertyName)) {
                errorAccumulator.add(targetPropertyName, new PreparationHasErrorException("New name already exists."))
            } else {
                resultDataFrame = dataFrame.withColumnRenamed(currentPropertyName, targetPropertyName)
            }
        }

        new Consequences(resultDataFrame, errorAccumulator)
    }
}
