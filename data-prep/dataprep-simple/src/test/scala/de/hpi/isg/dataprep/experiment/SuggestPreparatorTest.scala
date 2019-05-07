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

  override protected def beforeAll(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  "The decision engine" should "suggest a proper preparator as the first step" in {
    val resourcePath = getClass.getResource("/reformat_date/overall.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath)

    val decisionEngine = DecisionEngine.getInstance()
    decisionEngine.setPreparatorCandidates(
      Array("SuggestMergeProperty", "SuggestChangeDateFormat", "SuggestPivotTable", "SuggestRemovePreamble")
    )

    val acutalPreparator = decisionEngine.selectBestPreparator(pipeline)
    val expectedPreparator = new SuggestChangeDateFormat("Eingang Anmeldung", None, Option(DatePatternEnum.DayMonthYear))

    acutalPreparator shouldBe a [SuggestChangeDateFormat]
    val casted = acutalPreparator.asInstanceOf[SuggestChangeDateFormat]
    Assert.assertEquals(expectedPreparator.propertyName, casted.propertyName)
    Assert.assertEquals(expectedPreparator.sourceDatePattern, None)
    Assert.assertEquals(expectedPreparator.targetDatePattern, casted.targetDatePattern)
  }
}
