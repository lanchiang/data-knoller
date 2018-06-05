package de.hpi.isg.dataprep

import java.util.Map

import de.hpi.isg.dataprep.util.DatasetConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

/**
  * @author Lan Jiang
  * @since 2018/6/4
  */
object DatasetUtil {

  def createDataset(config: DatasetConfig) : DataFrame = {
      val spark = SparkSession.builder()
        .appName(config.getAppName)
        .master(config.getMaster)
        .getOrCreate()
      val options : java.util.Map[String, String] = config.getOptions
      var dataFrameReader = spark.read
      for ((optionName,optionValue) <- options) {
          dataFrameReader = dataFrameReader.option(optionName, optionValue)
      }
      config.getInputFileFormat match {
          case DatasetConfig.InputFileFormat.CSV => dataFrameReader.csv(config.getFilePath)
          case DatasetConfig.InputFileFormat.TEXTFILE => dataFrameReader.textFile(config.getFilePath).toDF()
      }
  }

}
