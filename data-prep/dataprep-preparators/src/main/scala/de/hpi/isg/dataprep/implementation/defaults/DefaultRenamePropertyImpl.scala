package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.Consequences
import de.hpi.isg.dataprep.implementation.abstracts.RenamePropertyImpl
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

        val newPropertyName = preparator_.getNewPropertyName
        val currentPropertyName = preparator_.getPropertyName

        val resultDataFrame = dataFrame.withColumnRenamed(currentPropertyName, newPropertyName)
        new Consequences(resultDataFrame, errorAccumulator)
    }
}
