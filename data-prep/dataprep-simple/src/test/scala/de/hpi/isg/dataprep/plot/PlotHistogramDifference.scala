package de.hpi.isg.dataprep.plot

import java.io.File

import de.hpi.isg.dataprep.experiment.define.RemovePreambleSuggest
import de.hpi.isg.dataprep.experiment.define.RemovePreambleSuggest.HISTOGRAM_ALGORITHM
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author Lan Jiang
  * @since 2019-04-09
  */
object PlotHistogramDifference {

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
      val histograms = dataset.map(row => RemovePreambleSuggest.valueLengthHistogram(row)).collect().toList

      // calculate the histogram difference of the neighbouring pairs of histograms
      val histogramDiff = histograms.sliding(2)
              .map(pair => RemovePreambleSuggest.histogramDifference(pair(0), pair(1), HISTOGRAM_ALGORITHM.Bhattacharyya))
              .toSeq
              .zipWithIndex
              .map(pair => {
                val x = (pair._2+1).toString.concat("||").concat((pair._2+2).toString)
                val y = pair._1
                (x,y)
              })

      histogramDiff.foreach(println)

      val histDF = histogramDiff.toDF()
      histDF.coalesce(1).write.mode(SaveMode.Overwrite).csv(outputPath + "/" + file.getName)
    }
  }
}
