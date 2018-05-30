package de.hpi.isg.dataprep.model.metadata.handler

import de.hpi.isg.dataprep.model.targets.{Metadata, Preparator}

/**
  * @author Lan Jiang
  * @since 2018/5/30
  */
object MetadataCoordinator {

  def validateMetadata(preparator: Preparator) : Set[Metadata] = {
    MetadataValidator.validateMetadata(preparator)
  }

  def readMetadata() : Set[Metadata] = {
    null
  }

  def updateMetadata(metadata : Set[Metadata]) : Boolean = {
    MetadataUpdater.updateMetadata(metadata)
    true
  }

  def extractMissingMetadata(metadata: Set[Metadata]) : Set[Metadata] = {
    null
  }
}
