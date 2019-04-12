package de.hpi.isg.dataprep.experiment

import de.hpi.isg.dataprep.experiment.define.RemovePreambleSuggest
import de.hpi.isg.dataprep.experiment.define.RemovePreambleSuggest.HISTOGRAM_ALGORITHM
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

/**
  * @author Lan Jiang
  * @since 2019-04-08
  */
class RemovePreambleSuggestTest extends FlatSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  var sparkContext:SparkSession = _

  override def beforeAll: Unit = {
    val sparkBuilder = SparkSession
            .builder()
            .appName("SparkTutorial")
            .master("local[4]")
    sparkContext = sparkBuilder.getOrCreate()
    super.beforeAll()
  }

  "A histogram indicating the value length of each field in a row" should "be returned" in {
    val localContext = sparkContext
    import localContext.implicits._
    val df = Seq(("what a sunny day today", 1, 2, "3")).toDF()
    val historgram = RemovePreambleSuggest.valueLengthHistogram(df.first())

    historgram shouldEqual Seq(0.88, 0.04, 0.04, 0.04)
  }

  "Encoder" should "be set correctly" in {
    val localContext = sparkContext
    import localContext.implicits._
    val df = Seq(("what a sunny day today", 1, 2, "3"), ("what a gloomy day", 33, 281, "123")).toDF()

    val histogram = df.map(row => RemovePreambleSuggest.valueLengthHistogram(row))
                    .collect()

    histogram shouldEqual Array(List(0.88, 0.04, 0.04, 0.04), List(0.68, 0.08, 0.12, 0.12))
  }

  "Histogram" should "be plotted correctly" in {

    val localContext = sparkContext
    import localContext.implicits._

    val dataset = Seq(("what a sunny day today", 1, 2, "3"), ("what a gloomy day", 33, 281, "123"), ("de.hpi.isg", 12, 333, "123")).toDF()

    // aggregate value length histogram of each row as an array
    val histograms = dataset.map(row => RemovePreambleSuggest.valueLengthHistogram(row)).collect().toList

    // calculate the histogram difference of the neighbouring pairs of histograms
    val histogramDiff = histograms.sliding(2)
            .map(pair => RemovePreambleSuggest.histogramDifference(pair(0), pair(1), HISTOGRAM_ALGORITHM.Chi_square))
            .toSeq
  }
}
