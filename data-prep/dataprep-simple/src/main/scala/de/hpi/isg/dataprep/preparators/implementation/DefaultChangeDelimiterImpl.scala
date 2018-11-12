package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.metadata.Delimiter
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.objects.TableMetadata
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangeDelimiter
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  * @author Lan Jiang
  * @since 2018/9/17
  */
class DefaultChangeDelimiterImpl extends PreparatorImpl {

//    override protected def executePreparator(preparator: AbstractPreparator, dataFrame: Dataset[Row]): ExecutionContext = {
//        val preparator_ = getPreparatorInstance(preparator, classOf[ChangeDelimiter])
//        val errorAccumulator = this.createErrorAccumulator(dataFrame)
//        executeLogic(preparator_, dataFrame, errorAccumulator)
//    }
//
//    protected def executeLogic(preparator: ChangeDelimiter, dataFrame: DataFrame,
//                               errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
//        val tableName = preparator.tableName
//        val targetDelimiter = preparator.targetDelimiter
//
//        // for the preparators that changes table-level metadata, simply update the metadata repository.
//        val delimiter = new Delimiter(targetDelimiter, new TableMetadata(tableName))
//
//        preparator.getPreparation.getPipeline.getMetadataRepository.updateMetadata(delimiter)
//
//        new ExecutionContext(dataFrame, errorAccumulator)
//    }

    override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
        val preparator = abstractPreparator.asInstanceOf[ChangeDelimiter]
        val tableName = preparator.tableName
        val targetDelimiter = preparator.targetDelimiter

        // for the preparators that changes table-level metadata, simply update the metadata repository.
        val delimiter = new Delimiter(targetDelimiter, new TableMetadata(tableName))

        preparator.getPreparation.getPipeline.getMetadataRepository.updateMetadata(delimiter)

        new ExecutionContext(dataFrame, errorAccumulator)
    }
}
