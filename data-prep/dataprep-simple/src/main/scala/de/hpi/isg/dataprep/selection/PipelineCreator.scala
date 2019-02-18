package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.components.Pipeline
import de.hpi.isg.dataprep.context.DataContext
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

/**
  * A PipelineCreator models the creation of a new pipeline for a given data context
  *
  * @param dataContext given data context
  */

abstract class PipelineCreator(dataContext: DataContext) {
  val preparators: List[Class[_ <: AbstractPreparator]] = PipelineCreator.preparators

  /**
    * Assembles a pipeline.
    *
    * @return finished Pipeline
    */
  def createPipeline(): Pipeline
}

object PipelineCreator extends PreparatorLoader {
  override val path: String = "de.hpi.isg.preparators"
}


