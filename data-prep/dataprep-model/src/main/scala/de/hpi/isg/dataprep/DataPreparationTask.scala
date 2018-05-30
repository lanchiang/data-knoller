package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.model.PreparatorExecutor
import de.hpi.isg.dataprep.model.metadata.handler.{MetadataCoordinator, MetadataRepository, MetadataValidator}
import de.hpi.isg.dataprep.model.provenance.ProvenanceRepository
import de.hpi.isg.dataprep.model.targets.{Preparator, Target}
import org.apache.spark.sql.DataFrame


/**
  * @author Lan Jiang
  * @since 2018/4/27
  */
class DataPreparationTask(df: DataFrame) {

  private var dataFrame = df
  private var provenanceRepository = new ProvenanceRepository()
  private var metadataRepository = new MetadataRepository()

  def applyPreparator(preparator: Preparator): Unit = {
    val invalidMetadata = MetadataCoordinator.validateMetadata(preparator)
    if (invalidMetadata.size > 0) {
      var extractedMetadata = MetadataCoordinator.extractMissingMetadata(invalidMetadata)
    }
    PreparatorExecutor.execute(preparator)
  }
}
