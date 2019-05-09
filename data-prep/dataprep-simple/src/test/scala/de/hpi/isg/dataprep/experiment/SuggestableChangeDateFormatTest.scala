package de.hpi.isg.dataprep.experiment

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.preparators.define.SuggestableChangeDateFormat
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import de.hpi.isg.dataprep.utils.SimplePipelineBuilder
import org.scalatest.{BeforeAndAfterEach, FlatSpecLike, Matchers}

/**
  * @author Lan Jiang
  * @since 2019-04-24
  */
class SuggestableChangeDateFormatTest extends FlatSpecLike with Matchers with BeforeAndAfterEach {

  "The decision engine" should "suggest a change date format preparator" in {
    val resourcePath = getClass.getResource("/reformat_date/overall.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath)

//    val preparator = new SuggestChangeDateFormat("Zahlung", None, Option(DatePatternEnum.DayMonthYear))
    val preparator = new SuggestableChangeDateFormat("Eingang Anmeldung", None, Option(DatePatternEnum.DayMonthYear))

    pipeline.addPreparation(new Preparation(preparator))
    pipeline.executePipeline

    pipeline.getDataset.show(100)
  }
}
