package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.model.target.Errorlog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DataType, IntegerType}

import scala.util.Try

/**
  * @author Lan Jiang
  * @since 2018/6/4
  */
object SparkPreparators {

    def renameColumn(dataFrame: DataFrame, columnName: String, newColumnName: String): DataFrame = {
        dataFrame.withColumnRenamed(columnName, newColumnName)
    }

    def changeColumnDataType(dataFrame: DataFrame, columnName: String, newDataType: Class[_ <: DataType]): DataFrame = {
        dataFrame.withColumn(columnName, dataFrame(columnName).cast(IntegerType))
//        dataFrame.select(columnName).flatMap(row => Try{convertStringToInteger(row)}.toOption)
//        dataFrame.withColumn(columnName, Try{udfConvert(dataFrame(columnName))})
    }

    val convertStringToInteger = (arg: String) => {
        try {
            arg.toInt
        } catch {
            case e : NumberFormatException => {
                var errorLog : Errorlog = new Errorlog(e.getClass.toString, arg)
                println(errorLog.toString)
                0
            }
        }
    }
    val udfConvert = udf(convertStringToInteger)

    def main(args: Array[String]): Unit = {
        val list = List("a","1","c","1.5","d","e","2")
        val result = list.flatMap(v => Try{convertStringToInteger(v)}.toOption)
        0
    }

}
