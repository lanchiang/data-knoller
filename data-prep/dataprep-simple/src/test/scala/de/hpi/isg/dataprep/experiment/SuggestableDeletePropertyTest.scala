package de.hpi.isg.dataprep.experiment

import de.hpi.isg.dataprep.components.{DecisionEngine, Preparation}
import de.hpi.isg.dataprep.preparators.define.SuggestableMergeProperty
import de.hpi.isg.dataprep.utils.SimplePipelineBuilder
import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

/**
  * @author Lan Jiang
  * @since 2019-05-09
  */
class SuggestableDeletePropertyTest extends FlatSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  val decisionEngine = DecisionEngine.getInstance()

  override protected def beforeAll(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  "The decision engine" should "suggest a delete property" in {
    val resourcePath = getClass.getResource("/reformat_date/overall.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath)

    decisionEngine.setPreparatorCandidates(
      Array("SuggestableDeleteProperty")
    )

    pipeline.addPreparation(new Preparation(new SuggestableMergeProperty(List("Rechnung", "Zahlung"))))

    val suggested = decisionEngine.selectBestPreparators(pipeline)

    None
  }
}
