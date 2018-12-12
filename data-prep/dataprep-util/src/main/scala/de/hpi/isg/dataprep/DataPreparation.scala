package de.hpi.isg.dataprep

import java.util.regex.Pattern

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * @author Lan Jiang
  * @since 17/01/2018
  */
class DataPreparation(df: DataFrame) {

  private var dataFrame = df

  private val copied_column_suffix = "_copied"

  //  def replaceSubstring(columnName: String, sourceValue: String, targetValue: String, k: Int=0): DataFrame = {
  //    var column = regexp_replace(dataFrame.col(columnName), sourceValue, targetValue)
  //    dataFrame
  //  }

  def getDataFrame(): DataFrame = {
    dataFrame
  }

  def renameColumn(columnName: String, newColumnName: String): DataPreparation = {
    dataFrame = dataFrame.withColumnRenamed(columnName, newColumnName)
    this
  }

  def addColumn(columnName: String, position: Int): DataPreparation = {
    this.addColumnWithDefaultValue(columnName, position, null)
  }

  def addColumnWithDefaultValue(columnName: String, position: Int, defaultValue: String): DataPreparation = {
    dataFrame = dataFrame.withColumn(columnName, lit(defaultValue))
    val columns = dataFrame.columns
    dataFrame = changeColumnPosition(columns, position)
    this
  }

  def copyColumn(columnName: String): DataPreparation = {
    val position = dataFrame.schema.fieldIndex(columnName) + 1
    copyColumn(columnName, position)
  }

  def copyColumn(columnName: String, position: Int): DataPreparation = {
    val copiedColumnName = columnName + copied_column_suffix
    dataFrame = dataFrame.withColumn(copiedColumnName, dataFrame.col(columnName))
    val columns = dataFrame.columns
    dataFrame = changeColumnPosition(columns, position)
    this
  }

  def moveColumn(columnName: String, newPosition: Int): DataPreparation = {
    // check whether the newPosition is out of range

    val currentPosition = dataFrame.schema.fieldIndex(columnName)
    if (currentPosition < newPosition) {
      copyColumn(columnName, newPosition)
    } else {
      copyColumn(columnName, newPosition - 1)
    }
    this.deleteColumn(columnName)
    val copied_name = columnName + copied_column_suffix
    this.renameColumn(copied_name, columnName)
  }

  def moveColumnToHead(columnName: String): DataPreparation = {
    moveColumn(columnName, 1)
  }

  def moveColumnToTail(columnName: String): DataPreparation = {
    val schemaLength = dataFrame.schema.fields.length
    moveColumn(columnName, schemaLength)
  }

  def deleteColumn(columnName: String): DataPreparation = {
    dataFrame = dataFrame.drop(columnName)
    this
  }

  def sortByColumnAscending(columnName: String): DataPreparation = {
    dataFrame = dataFrame.sort(dataFrame.col(columnName).asc)
    this
  }

  def sortByColumnDescending(columnName: String): DataPreparation = {
    dataFrame = dataFrame.sort(dataFrame.col(columnName).desc)
    this
  }

  def replaceConstSubstring(columnName: String, sourceValue: String, targetValue: String, k: Int = 0): DataPreparation = {
    // if k is zero, replace all the found substring with the new string
    dataFrame = dataFrame.withColumn(columnName, regexp_replace(col(columnName), sourceValue, targetValue))
    this
  }

  def replaceSubstringWithRegularExpression(columnName: String, pattern: Pattern, targetValue: String, k: Int = 0): DataPreparation = {
    // if k is zero, replace all the found substring with the new string
    dataFrame = dataFrame.withColumn(columnName, regexp_replace(col(columnName), pattern.pattern(), targetValue))
    this
  }

  def removeNumericCharacters(columnName: String): DataPreparation = {
    val pattern = Pattern.compile("\\d+")
    this.replaceSubstringWithRegularExpression(columnName, pattern, "")
  }

  def removeNumericCharacters(): DataPreparation = {
    dataFrame.columns.map(columnName => removeNumericCharacters(columnName))
    this
  }

  def removeNonNumerics(columnName: String): DataPreparation = {
    val pattern = Pattern.compile("[^0-9]")
    this.replaceSubstringWithRegularExpression(columnName, pattern, "")
  }

  def removeNonNumerics(): DataPreparation = {
    dataFrame.columns.map(columnName => removeNonNumerics(columnName))
    this
  }

  def removeNonAlphaNumerics(columnName: String): DataPreparation = {
    val pattern = Pattern.compile("[^a-zA-Z0-9]")
    this.replaceSubstringWithRegularExpression(columnName, pattern, "")
  }

  def removeNonAlphaNumerics(): DataPreparation = {
    dataFrame.columns.map(columnName => removeNonAlphaNumerics(columnName))
    this
  }

  def removeString(columnName: String, toBeRemovedString: String): DataPreparation = {
    this.replaceConstSubstring(columnName, toBeRemovedString, "")
  }

  def removeString(toBeRemovedString: String): DataPreparation = {
    dataFrame.columns.map(columnName => removeString(columnName, toBeRemovedString))
    this
  }

  def fillNull(columnName: String, filling: String): DataPreparation = {
    // now consider the build-in null, in the future read the null value from metadata
    dataFrame = dataFrame.withColumn(columnName, when(dataFrame.col(columnName).isNull, filling).otherwise(col(columnName)))
    this
  }

  def fillDownNull(columnName: String): DataPreparation = {
    //df.withColumn("id", func.last('id', True).over(Window.partitionBy('session').orderBy('ts').rowsBetween(-sys.maxsize, 0))).show()

    this
  }

  def hash(columnName: String, algorithm: String): DataPreparation = {
    val caseInsensitiveAlgorithmName = algorithm.toUpperCase
    var newColumnName = columnName
    caseInsensitiveAlgorithmName match {
      case "MD5" => {
        newColumnName += "_md5"
        dataFrame = dataFrame.withColumn(newColumnName, md5(dataFrame.col(columnName)))
      }
      case "SHA1" => {
        newColumnName += "_sha1"
        dataFrame = dataFrame.withColumn(newColumnName, sha1(dataFrame.col(columnName)))
      }
    }
    val position = dataFrame.schema.fieldIndex(columnName) + 1
    moveColumn(newColumnName, position)
    this
  }

  def phoneticHash(columnName: String, algorithm: String): DataPreparation = {
    val caseInsensitiveAlgorithmName = algorithm.toUpperCase
    var newColumnName = columnName
    caseInsensitiveAlgorithmName match {
      case "SOUNDEX" => {
        newColumnName += "_soundex"
        dataFrame = dataFrame.withColumn(newColumnName, soundex(dataFrame.col(columnName).cast(StringType)))
      }
    }
    this
  }

  def lowerCase(columnName: String): DataPreparation = {
    dataFrame = dataFrame.withColumn(columnName, lower(dataFrame.col(columnName)))
    this
  }

  def upperCase(columnName: String): DataPreparation = {
    dataFrame = dataFrame.withColumn(columnName, upper(dataFrame.col(columnName)))
    this
  }

  def letterCase(columnName: String): DataPreparation = {
    dataFrame = dataFrame.withColumn(columnName, initcap(dataFrame.col(columnName)))
    this
  }

  def trimColumn(columnName: String): DataPreparation = {
    dataFrame = dataFrame.withColumn(columnName, trim(col(columnName)))
    this
  }

  def addAutoIncreasingIdColumn(): DataPreparation = {
    val columnName = "auto_increasing_id"
    try {
      val fieldIndex = dataFrame.schema.fieldIndex(columnName)
      println("The auto_increasing_id column already exists!")
    } catch {
      case ex: IllegalArgumentException => {
        dataFrame = dataFrame.withColumn(columnName, monotonically_increasing_id())
        moveColumnToHead(columnName)
      }
    }
    this
  }

  def changeColumnDataType(columnName: String, dataType: DataType): DataPreparation = {
    val currentDataType = dataFrame.dtypes
      .filter(tuple => tuple._1.equals(columnName))
      .map(tuple => tuple._2).head
    if (dataType.isInstanceOf[NumericType]) {
      dataFrame = dataFrame.withColumn(columnName, dataFrame.col(columnName).cast(dataType))
    } else if (dataType.isInstanceOf[StringType]) {
      dataFrame = dataFrame.withColumn(columnName, dataFrame.col(columnName).cast(dataType))
    } else {

    }
    this
  }

  def forContrast(columnName: String): DataPreparation = {
    dataFrame = dataFrame.select(dataFrame.col(columnName).alias("Name"), md5(dataFrame.col(columnName).cast(StringType)).alias("New_Name"))
    this
  }

  private def changeColumnPosition(columns: Array[String], position: Int): DataFrame = {
    val headPart = columns.slice(0, position)
    val tailPart = columns.slice(position, columns.length - 1)
    val reorderedColumnNames = headPart ++ columns.slice(columns.length - 1, columns.length) ++ tailPart
    dataFrame.select(reorderedColumnNames.map(column => col(column)): _*)
  }
}

object DataPreparation {

  def main(args: Array[String]): Unit = {

  }
}
