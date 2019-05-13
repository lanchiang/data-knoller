package de.hpi.isg.dataprep.experiment

import de.hpi.isg.dataprep.components.DecisionEngine
import de.hpi.isg.dataprep.utils.SimplePipelineBuilder
import org.junit.Assert
import org.scalatest.{BeforeAndAfterEach, FlatSpecLike, Matchers}

/**
  * @author Lan Jiang
  * @since 2019-05-13
  */
class SuggestableFilterTest extends FlatSpecLike with Matchers with BeforeAndAfterEach {

  "The calApplicability" should "returns correct score of filter preparator for filter_test1.csv" in {
    val resourcePath = getClass.getResource("/filter/filter_test1.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath, false)

    val decisionEngine = DecisionEngine.getInstance()
    decisionEngine.setPreparatorCandidates(Array("SuggestableFilter"))

    val actualPreparators = decisionEngine.selectBestPreparators(pipeline).getSuggestedPreparators.toSeq

    Assert.assertEquals(actualPreparators.length, 0)
  }

  "The calApplicability" should "returns correct score of filter preparator for filter_test2.csv" in {
    val resourcePath = getClass.getResource("/filter/filter_test2.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath, false)

    val decisionEngine = DecisionEngine.getInstance()
    decisionEngine.setPreparatorCandidates(Array("SuggestableFilter"))

    val actualPreparator = decisionEngine.selectBestPreparators(pipeline).getSuggestedPreparators.toSeq(0).getPreparator

    None
  }
}
