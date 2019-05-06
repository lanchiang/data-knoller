package de.hpi.isg.dataprep.experiment

import de.hpi.isg.dataprep.components.DecisionEngine
import de.hpi.isg.dataprep.preparators.define.SuggestMergeProperty
import de.hpi.isg.dataprep.utils.SimplePipelineBuilder
import org.junit.Assert
import org.scalatest.{BeforeAndAfterEach, FlatSpecLike, Matchers}

/**
  * @author Lan Jiang
  * @since 2019-04-17
  */
class SuggestMergePropertyTest extends FlatSpecLike with Matchers with BeforeAndAfterEach {

  "The decision engine" should "suggest a merge property preparator for two interleaving columns" in {
    val resourcePath = getClass.getResource("/merge_property/interleaving_string.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath, false)

    val decisionEngine = DecisionEngine.getInstance()
    decisionEngine.setPreparatorCandidates(Array("SuggestMergeProperty"))

    val actualPreparator = decisionEngine.selectBestPreparators(pipeline)
    val expectedPreparator = new SuggestMergeProperty(Array("_c0", "_c1").toList, null)

    actualPreparator shouldBe a [SuggestMergeProperty]
    val casted = actualPreparator.asInstanceOf[SuggestMergeProperty]
    Assert.assertEquals(expectedPreparator.attributes, casted.attributes)
    Assert.assertEquals(expectedPreparator.connector, casted.connector)
  }
}
