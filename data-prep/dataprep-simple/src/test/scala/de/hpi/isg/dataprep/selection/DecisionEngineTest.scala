package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.DeleteProperty

import scala.collection.JavaConverters._

class DecisionEngineTest extends DataLoadingConfig {

  "The decision engine" should "find the correct prepartor" in {
    val decisionEngine = new SimpleDecisionEngine()
    val expectedPreparator = new DeleteProperty("date")

    val preparator = decisionEngine.selectNextPreparation(List[Class[_ <: AbstractPreparator]](classOf[DeleteProperty]), pipeline.getSchemaMapping, pipeline.getRawData, pipeline.getTargetMetadata.asScala.toList).get

    pipeline.addPreparation(new Preparation(preparator))
    preparator shouldEqual expectedPreparator
  }

}
