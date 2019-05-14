package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.{DataFile, Delimiter, ExecutionContext, Property, Table}
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangeDelimiter
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator


/**
  * @author Lan Jiang
  * @since 2018/9/17
  */
class DefaultChangeDelimiterImpl extends AbstractPreparatorImpl {

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[ChangeDelimiter]
    val tableName = preparator.tableName
    val targetDelimiter = preparator.targetDelimiter

    // for the preparators that changes table-level metadata, simply update the metadata repository.
    val delimiter = new Delimiter(new DataFile("fake_filename", new Table(tableName, Array.empty[Property])), targetDelimiter)

    preparator.getPreparation.getPipeline.getMetadataRepository.update(delimiter)

    new ExecutionContext(dataFrame, errorAccumulator)
  }
}
