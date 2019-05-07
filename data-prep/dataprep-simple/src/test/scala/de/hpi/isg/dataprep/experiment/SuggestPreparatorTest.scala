package de.hpi.isg.dataprep.experiment

import de.hpi.isg.dataprep.components.DecisionEngine
import de.hpi.isg.dataprep.preparators.define.SuggestChangeDateFormat
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import de.hpi.isg.dataprep.utils.SimplePipelineBuilder
import org.apache.log4j.{Level, Logger}
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

/**
  * @author Lan Jiang
  * @since 2019-05-06
  */
class SuggestPreparatorTest extends FlatSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  val decisionEngine = DecisionEngine.getInstance()

  override protected def beforeAll(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  "The decision engine" should "suggest a proper preparator as the first step for overall.csv dataset" in {
    val resourcePath = getClass.getResource("/suggestion/overall.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath)

    decisionEngine.setPreparatorCandidates(
      Array("SuggestMergeProperty", "SuggestChangeDateFormat", "SuggestPivotTable", "SuggestRemovePreamble")
    )

    val actualPreparator = decisionEngine.selectBestPreparator(pipeline)
    val expectedPreparator = new SuggestChangeDateFormat("Eingang Anmeldung", None, Option(DatePatternEnum.DayMonthYear))

    actualPreparator shouldBe a [SuggestChangeDateFormat]
    val casted = actualPreparator.asInstanceOf[SuggestChangeDateFormat]
    Assert.assertEquals(expectedPreparator.propertyName, casted.propertyName)
    Assert.assertEquals(expectedPreparator.sourceDatePattern, None)
    Assert.assertEquals(expectedPreparator.targetDatePattern, casted.targetDatePattern)
  }

  "The decision engine" should "suggest a list of preparators for the first preparation step for 14_16_Feb_DD_agile.csv dataset" in {
    val resourcePath = getClass.getResource("/suggestion/14_16_Feb_DD_agile.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath, false);

    decisionEngine.setPreparatorCandidates(
      Array("SuggestMergeProperty", "SuggestChangeDateFormat", "SuggestPivotTable", "SuggestRemovePreamble")
    )

    val suggestedList = decisionEngine.selectBestPreparators(pipeline)
    println(suggestedList.getSuggestedPreparators)
  }
}
