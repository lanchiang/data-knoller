package de.hpi.isg.dataprep.model.metadata.handler

import de.hpi.isg.dataprep.model.targets.{Metadata, Preparator}

/**
  * @author Lan Jiang
  * @since 2018/4/27
  */
private object MetadataValidator {

  /**
    * Checks whether the required metadata to execute this preparator are valid.
    * @param preparator Refers to the preparator whose metadata requirements need to be checked.
    * @return
    */
  def validateMetadata(preparator: Preparator): Set[Metadata] = {
    var invalidMetadata: Set[Metadata] = Set()
    invalidMetadata
  }
}
