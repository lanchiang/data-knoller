package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.components.{Pipeline, Preparation}
import de.hpi.isg.dataprep.context.DataContext

abstract class PipelineCreator(dataContext: DataContext) extends PreparatorLoader {
  def createPipeline(): Pipeline
}

class SimplePipelineCreator(dataContext: DataContext) extends PipelineCreator(dataContext)  {
  override val path: String = "de.hpi.isg.preparators"

  private val MAX_ITERATIONS = 100
  private val engine = new SimpleDecisionEngine()

  override def createPipeline(): Pipeline = {
    val pipe = new Pipeline(dataContext)


    for(_ <- 0 to MAX_ITERATIONS) {
      engine.selectNextPreparation(preparator.toList, dataContext.getSchemaMapping, dataContext.getDataFrame, dataContext.getTargetMetadata) match {
        case Some(prep) => pipe.addPreparation(new Preparation(prep))
        case None => return pipe
      }
    }
    pipe
  }
}
