package de.hpi.isg.dataprep.experiment

import java.io.{BufferedWriter, FileWriter, IOException}

import de.hpi.isg.dataprep.components.DecisionEngine
import de.hpi.isg.dataprep.preparators.define.SuggestableChangeDateFormat
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
      Array("SuggestableMergeProperty", "SuggestableChangeDateFormat", "SuggestablePivotTable", "SuggestableRemovePreamble")
    )

    val actualPreparator = decisionEngine.selectBestPreparator(pipeline)
    val expectedPreparator = new SuggestableChangeDateFormat("Eingang Anmeldung", None, Option(DatePatternEnum.DayMonthYear))

    actualPreparator shouldBe a [SuggestableChangeDateFormat]
    val casted = actualPreparator.asInstanceOf[SuggestableChangeDateFormat]
    Assert.assertEquals(expectedPreparator.propertyName, casted.propertyName)
    Assert.assertEquals(expectedPreparator.sourceDatePattern, None)
    Assert.assertEquals(expectedPreparator.targetDatePattern, casted.targetDatePattern)
  }

  "The decision engine" should "suggest a list of preparators for the first preparation step for 14_16_Feb_DD_agile.csv dataset" in {
    val resourcePath = getClass.getResource("/suggestion/14_16_Feb_DD_agile.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath, false)

    decisionEngine.setPreparatorCandidates(
      Array("SuggestableMergeProperty", "SuggestableChangeDateFormat", "SuggestableRemovePreamble")
    )

    val suggestedList = decisionEngine.selectBestPreparators(pipeline)

    // Todo: this is just for showing experiments, remove it after shown.
    try {
      val bufferedWriter = new BufferedWriter(
        new FileWriter("/Users/Fuga/Documents/HPI/data-preparation/Suggestion/results/first_preparator_suggestlist"
                .concat("/14_16_Feb_DD_agile.txt")))
      for (suggestedPreparator <- suggestedList.getSuggestedPreparators) {
        bufferedWriter.write(suggestedPreparator.toString)
        bufferedWriter.newLine()
      }
      bufferedWriter.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  "The decision engine" should "suggest a list of preparators for the first preparation step for NCHS_-_Death_rates_and_life_expectancy_at_birth.csv dataset" in {
    val resourcePath = getClass.getResource("/suggestion/NCHS_-_Death_rates_and_life_expectancy_at_birth.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath)

    decisionEngine.setPreparatorCandidates(
      Array("SuggestableMergeProperty", "SuggestableChangeDateFormat", "SuggestablePivotTable", "SuggestableRemovePreamble")
    )

    val suggestedList = decisionEngine.selectBestPreparators(pipeline)

    // Todo: this is just for showing experiments, remove it after shown.
    try {
      val bufferedWriter = new BufferedWriter(
        new FileWriter("/Users/Fuga/Documents/HPI/data-preparation/Suggestion/results/first_preparator_suggestlist"
                .concat("/NCHS_-_Death_rates_and_life_expectancy_at_birth.txt")))
      for (suggestedPreparator <- suggestedList.getSuggestedPreparators) {
        bufferedWriter.write(suggestedPreparator.toString)
        bufferedWriter.newLine()
      }
      bufferedWriter.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }
}
