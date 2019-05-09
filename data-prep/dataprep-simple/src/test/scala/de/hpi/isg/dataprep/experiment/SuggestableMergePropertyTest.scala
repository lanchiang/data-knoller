package de.hpi.isg.dataprep.experiment

import de.hpi.isg.dataprep.components.DecisionEngine
import de.hpi.isg.dataprep.preparators.define.SuggestableMergeProperty
import de.hpi.isg.dataprep.utils.SimplePipelineBuilder
import org.junit.Assert
import org.scalatest.{BeforeAndAfterEach, FlatSpecLike, Matchers}

/**
  * @author Lan Jiang
  * @since 2019-04-17
  */
class SuggestableMergePropertyTest extends FlatSpecLike with Matchers with BeforeAndAfterEach {

  "The decision engine" should "suggest a merge property preparator for two interleaving columns" in {
    val resourcePath = getClass.getResource("/merge_property/interleaving_string.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath, false)

    val decisionEngine = DecisionEngine.getInstance()
    decisionEngine.setPreparatorCandidates(Array("SuggestableMergeProperty"))

    val actualPreparator = decisionEngine.selectBestPreparators(pipeline).getSuggestedPreparators.toSeq(0).getPreparator
    val expectedPreparator = new SuggestableMergeProperty(Array("_c0", "_c1").toList, null)

    actualPreparator shouldBe a [SuggestableMergeProperty]
    val casted = actualPreparator.asInstanceOf[SuggestableMergeProperty]
    Assert.assertEquals(expectedPreparator.attributes, casted.attributes)
    Assert.assertEquals(expectedPreparator.connector, casted.connector)
  }
}
