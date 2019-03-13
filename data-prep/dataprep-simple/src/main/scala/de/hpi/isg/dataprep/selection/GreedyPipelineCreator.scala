package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.components.{Pipeline, Preparation}
import de.hpi.isg.dataprep.context.DataContext

/**
  * The greedy pipeline creator builds a pipeline with the greedy approach of the decision engine
  *
  * @param dataContext given data context
  */

class GreedyPipelineCreator(dataContext: DataContext) extends PipelineCreator(dataContext) {
  private val MAX_ITERATIONS = 100
  private val engine = new DecisionEngine()

  /**
    * Assembles a pipeline.
    *
    * @return finished Pipeline
    */
  override def createPipeline(): Pipeline = {
    val pipe = new Pipeline(dataContext)
    for (_ <- 0 to MAX_ITERATIONS) {
      engine.selectNextPreparation(preparators, dataContext.getSchemaMapping, dataContext.getDataFrame, dataContext.getTargetMetadata) match {
        case Some(prep) => pipe.addPreparation(new Preparation(prep))
        case None => return pipe
      }
    }
    pipe
  }
}
