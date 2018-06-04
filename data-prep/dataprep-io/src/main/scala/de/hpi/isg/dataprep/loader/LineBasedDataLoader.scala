package de.hpi.isg.dataprep.loader

import de.hpi.isg.dataprep.CommonLoaderSettings
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * @author Lan Jiang
  * @since 2018/5/31
  */
class LineBasedDataLoader(settings : CommonLoaderSettings) extends DataLoader(settings) {

  /**
    * Read the file as lines.
    * @return
    */
  def readAsLines() : List[String] = {
    val filename = settings.file_url
    var lines : ListBuffer[String] = ListBuffer()
    for (line <- Source.fromFile(filename).getLines()) {
      lines += line
    }
    lines.toList
  }

  def readLinesWithSpark(): DataFrame = {
    val filename = settings.file_url
    var lines : ListBuffer[String] = ListBuffer()
    val spark = SparkSession.builder().appName("Name").master("local").getOrCreate()
    val dataframe = spark.read.text(filename)
    dataframe
  }
}

object LineBasedDataLoader {

  def main(args: Array[String]): Unit = {
    var settings : CommonLoaderSettings = new CommonLoaderSettings()
    settings.file_url = "/Users/Fuga/Documents/HPI/data/cora/cora.csv"
    var dataloader : LineBasedDataLoader = new LineBasedDataLoader(settings)
    dataloader.readLinesWithSpark()
  }
}