package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.repository.MetadataRepository
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog

class Preparation(val preparator: Preparator) {
  /**
    * Check whether this preparator along with the previous one cause pipeline-level errors.
    *
    * @param metadataRepository is the instance of the metadata repository of this pipeline.
    * @return true if there is at least one pipeline-level error.
    */
  def checkPipelineErrorWithPrevious(metadataRepository: MetadataRepository): ErrorLog = ???

  /**
    * Get the name of the instance.
    *
    * @return the name
    */
  def getName: String = preparator.name
}
