package de.hpi.isg.dataprep.plot

import java.io.File

import de.hpi.isg.dataprep.preparators.define.SuggestableRemovePreamble
import de.hpi.isg.dataprep.preparators.define.SuggestableRemovePreamble.HISTOGRAM_ALGORITHM
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * @author Lan Jiang
  * @since 2019-04-09
  */
object PlotHistogramDifferenceTest {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val inputFiles = new File(getClass.getResource("/preambled").toURI.getPath).listFiles
    val outputPath = new File("histogram")

    val sparkBuilder = SparkSession
            .builder()
            .appName("SparkTutorial")
            .master("local[4]")

    val localContext = sparkBuilder.getOrCreate()
    import localContext.implicits._

    for (file <- inputFiles) {
      val dataset = localContext.read
              .option("sep", ",")
              .csv(file.getAbsolutePath)

      dataset.show()

      // aggregate value length histogram of each row as an array
      //    val histograms = withoutEmptyLineDF.map(row => SuggestableRemovePreamble.valueLengthHistogram(row)).collect()
      val histograms = dataset.map(row => SuggestableRemovePreamble.valueLengthHistogram(row)).collect()

      var lastNonempty = Seq.empty[Double]
      var nonEmptyHistograms = Array.empty[Seq[Double]]
      for (histogram <- histograms) {
        if (histogram.exists(_ != 0)) {
          lastNonempty = histogram
        }
        nonEmptyHistograms = nonEmptyHistograms :+ lastNonempty
      }

      // calculate the histogram difference of the neighbouring pairs of histograms, zipping them with index
      val histogramDifference = nonEmptyHistograms
              .sliding(2)
              .map(pair => SuggestableRemovePreamble.histogramDifference(pair(0), pair(1), HISTOGRAM_ALGORITHM.Bhattacharyya))
              .toSeq
      val (min, max) = (histogramDifference.min, histogramDifference.max)
      val histogramDiff = histogramDifference
              .map(diff => (diff-min)/(max-min))
              .zipWithIndex
              .map(pair => {
                val x = (pair._2+1).toString.concat("||").concat((pair._2+2).toString)
                val y = pair._1
                (x,y)
              })
//              .sortBy(indexedHistDiff => indexedHistDiff._2)(Ordering[Double].reverse)

      // Gets score 1 if the cliff is larger than the threshold.
      val histDiffCliff = histogramDiff
              .sliding(2)
              .map(pair => (pair(0)._1 , pair(0)._2 - pair(1)._2))
              .maxBy(drop => drop._2)

//      val nonEmptyLineDf = dataset.na.drop("all")
//
//      // aggregate value length histogram of each row as an array
//      val histograms = nonEmptyLineDf.map(row => SuggestableRemovePreamble.valueLengthHistogram(row)).collect().toList
//
//      // calculate the histogram difference of the neighbouring pairs of histograms
//      val histogramDifference = histograms.sliding(2)
//              .map(pair => SuggestableRemovePreamble.histogramDifference(pair(0), pair(1), HISTOGRAM_ALGORITHM.Bhattacharyya))
//              .toSeq
//      val (min, max) = (histogramDifference.min, histogramDifference.max)
//
//      val histogramDiff = histogramDifference
//              .map(diff => (diff-min)/(max-min))
//              .zipWithIndex
//              .map(pair => {
//                val x = (pair._2+1).toString.concat("||").concat((pair._2+2).toString)
//                val y = pair._1
//                (x,y)
//              })
//              .sortBy(indexedHistDiff => indexedHistDiff._2)(Ordering[Double].reverse)
//
////      histogramDiff.foreach(println)
//
//      val histDiffCliff = histogramDiff
//              .sliding(2)
//              .map(pair => (pair(0)._1 , pair(0)._2 - pair(1)._2))
//              .maxBy(drop => drop._2)

      println(file.getName + histDiffCliff)

      // Todo: find the border by considering the target

      // write the score list to a csv file
      val histDF = histogramDiff.toDF()
      histDF.coalesce(1).write.mode(SaveMode.Overwrite).csv(outputPath + "/" + file.getName)
    }
  }

  def nullCount(row: Row): Int = {
    row.toSeq.filter(cell => cell != null).size
  }
}
