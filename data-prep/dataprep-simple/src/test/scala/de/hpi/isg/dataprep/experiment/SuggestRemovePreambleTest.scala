package de.hpi.isg.dataprep.experiment

import de.hpi.isg.dataprep.preparators.define.SuggestRemovePreamble
import de.hpi.isg.dataprep.preparators.define.SuggestRemovePreamble.HISTOGRAM_ALGORITHM
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

/**
  * @author Lan Jiang
  * @since 2019-04-08
  */
class SuggestRemovePreambleTest extends FlatSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  var sparkContext:SparkSession = _

  override def beforeAll: Unit = {
    val sparkBuilder = SparkSession
            .builder()
            .appName("SuggestRemovePreambleTest")
            .master("local[4]")
    sparkContext = sparkBuilder.getOrCreate()
    super.beforeAll()
  }

  "A histogram indicating the value length of each field in a row" should "be returned" in {
    val localContext = sparkContext
    import localContext.implicits._
    val df = Seq(("what a sunny day today", 1, 2, "3")).toDF()
    val historgram = SuggestRemovePreamble.valueLengthHistogram(df.first())

    historgram shouldEqual Seq(22.0, 1.0, 1.0, 1.0)
  }

  "Encoder" should "be set correctly" in {
    val localContext = sparkContext
    import localContext.implicits._
    val df = Seq(("what a sunny day today", 1, 2, "3"), ("what a gloomy day", 33, 281, "123")).toDF()

    val histogram = df.map(row => SuggestRemovePreamble.valueLengthHistogram(row))
                    .collect()

    histogram shouldEqual Array(List(22.0, 1.0, 1.0, 1.0), List(17.0, 2.0, 3.0, 3.0))
  }

  "Histogram" should "be plotted correctly" in {

    val localContext = sparkContext
    import localContext.implicits._

    val dataset = Seq(("what a sunny day today", 1, 2, "3"), ("what a gloomy day", 33, 281, "123"), ("de.hpi.isg", 12, 333, "123")).toDF()

    // aggregate value length histogram of each row as an array
    val histograms = dataset.map(row => SuggestRemovePreamble.valueLengthHistogram(row)).collect().toList

    // calculate the histogram difference of the neighbouring pairs of histograms
    val histogramDiff = histograms.sliding(2)
            .map(pair => SuggestRemovePreamble.histogramDifference(pair(0), pair(1), HISTOGRAM_ALGORITHM.Chi_square))
            .toSeq
  }
}
