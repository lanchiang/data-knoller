package de.hpi.isg.dataprep.model.metadata.handler

import de.hpi.isg.dataprep.model.targets.Metadata

/**
  * @author Lan Jiang
  * @since 2018/5/29
  */
private object MetadataUpdater {

  /**
    * Update the metadata after executing a preparator.
    * @param metadata represents set of the metadata to be update.
    * @return mark whether the process succeeds.
    */
  def updateMetadata(metadata: Set[Metadata]): Boolean = {
    var successful = false
    successful
  }
}
