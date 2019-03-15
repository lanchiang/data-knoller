package de.hpi.isg.dataprep

import java.nio.charset.StandardCharsets
import java.util.Map

import de.hpi.isg.dataprep.util.DatasetConfig
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * @author Lan Jiang
  * @since 2018/6/4
  */
// Todo: this class can be moved to dataprep-spark module.
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

  @throws(classOf[UnsupportedOperationException])
  @throws(classOf[IllegalArgumentException])
  def getFieldIndexByPropertyNameSafe(row: Row, propertyName: String): Int = {
    val indexTry = Try {
      row.fieldIndex(propertyName)
    }
    val index = indexTry match {
      case Failure(content) => {
        throw content
      }
      case Success(content) => content
    }
    index
  }

  def getValueByFieldIndex(row: Row, index: Int, dataType: DataType): Any = {
    val created = dataType match {
      case IntegerType => row.getInt(index)
      case StringType => row.getString(index)
    }
    created
  }
}
