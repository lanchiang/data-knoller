package de.hpi.isg.dataprep.experiment

import de.hpi.isg.dataprep.components.DecisionEngine
import de.hpi.isg.dataprep.preparators.define.SuggestPivotTable
import de.hpi.isg.dataprep.utils.SimplePipelineBuilder
import org.scalatest.{BeforeAndAfterEach, FlatSpecLike, Matchers}

/**
  * @author Lan Jiang
  * @since 2019-05-06
  */
class SuggestPivotTableTest extends FlatSpecLike with Matchers with BeforeAndAfterEach {

  "The decision engine" should "given correct score for pivot table preparator suggestion" in {
    val resourcePath = getClass.getResource("/pivot/pivot-test1.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath, false)

    val decisionEngine = DecisionEngine.getInstance()
    decisionEngine.setPreparatorCandidates(Array("SuggestPivotTable"))

    val actualPreparator = decisionEngine.selectBestPreparator(pipeline)
  }
}
