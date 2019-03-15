package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.DeleteProperty


class DecisionEngineTest extends DataLoadingConfig {

  "The decision engine" should "find the correct prepartor" in {
    val decisionEngine = new DecisionEngine()
    val expectedPreparator = new DeleteProperty("date")

    val preparator = decisionEngine.selectNextPreparation(List[Class[_ <: AbstractPreparator]](classOf[DeleteProperty]), pipeline.getSchemaMapping, pipeline.getDataset, pipeline.getTargetMetadata).get

    pipeline.addPreparation(new Preparation(preparator))
    preparator shouldEqual expectedPreparator
  }

}
