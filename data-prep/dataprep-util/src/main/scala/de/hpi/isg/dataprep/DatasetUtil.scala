package de.hpi.isg.dataprep

import java.nio.charset.StandardCharsets
import java.util.Map

import de.hpi.isg.dataprep.util.DatasetConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

/**
  * @author Lan Jiang
  * @since 2018/6/4
  */
object DatasetUtil {

    def createDataset(config: DatasetConfig): DataFrame = {
        val spark = SparkSession.builder()
                .appName(config.getAppName)
                .master(config.getMaster)
                .getOrCreate()
        val options: java.util.Map[String, String] = config.getOptions
        var dataFrameReader = spark.read
        for ((optionName, optionValue) <- options) {
            dataFrameReader = dataFrameReader.option(optionName, optionValue)
        }
        //      config.getInputFileFormat match {
        //          case DatasetConfig.InputFileFormat.CSV => dataFrameReader.csv(config.getFilePath)
        //          case DatasetConfig.InputFileFormat.TEXTFILE => dataFrameReader.textFile(config.getFilePath).toDF()
        //      }
        dataFrameReader.textFile(config.getFilePath).toDF();
    }

    def changeFileEncoding(config: DatasetConfig): DataFrame = {
        val spark = SparkSession.builder()
                .appName(config.getAppName)
                .master(config.getMaster)
                .getOrCreate()
        import spark.sqlContext.implicits._
        spark.sparkContext.binaryFiles(config.getFilePath)
                .mapValues(content => new String(content.toArray(), StandardCharsets.ISO_8859_1))
                .toDF()
    }
}
