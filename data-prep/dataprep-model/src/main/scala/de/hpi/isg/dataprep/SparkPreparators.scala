package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.model.target.ErrorLog
import org.apache.spark.sql.{ColumnName, DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * @author Lan Jiang
  * @since 2018/6/4
  */
object SparkPreparators {

    val spark = SparkSession.builder()
            .appName("Test")
            .master("local")
            .getOrCreate()

    def renameColumn(dataFrame: DataFrame, columnName: String, newColumnName: String): DataFrame = {
        dataFrame.withColumnRenamed(columnName, newColumnName)
    }

    def changeColumnDataType(dataFrame: DataFrame, columnName: String, newDataType: Class[_ <: DataType]): DataFrame = {
        dataFrame.withColumn(columnName, dataFrame(columnName).cast(IntegerType))
//        dataFrame.select(columnName).flatMap(row => Try{convertStringToInteger(row)}.toOption)
//        dataFrame.withColumn(columnName, Try{udfConvert(dataFrame(columnName))})
    }

    def changeColumnDataType(dataFrame: DataFrame, columnName: String, targetDataType: String): DataFrame = {
//        spark.sparkContext
        import spark.implicits._
        dataFrame.map(row => row.toString())
                .flatMap(string => Try{string.toInt}.toOption).toDF()
    }

    val convertStringToInteger = (arg: String) => {
        try {
            arg.toInt
        } catch {
            case e : NumberFormatException => {
//                var errorLog : ErrorLog = new ErrorLog(e.getClass.toString, arg)
//                println(errorLog.toString)
                0
            }
        }
    }
    val udfConvert = udf(convertStringToInteger)

    def main(args: Array[String]): Unit = {
        val list = List("a","1","c","1.5","d","e","2")
//        val result = list.flatMap(v => Try{convertStringToInteger(v)}.toOption)
//        System.out.println(result)
//        0
        val accumulatorV2 = new CollectionAccumulator[(Any, Throwable)]
        spark.sparkContext.register(accumulatorV2, "test")
        val transformed = list.flatMap(e => {
            val fe = Try{e.toInt}
            val trial = fe match {
                case Failure(t) =>
                    accumulatorV2.add(e, t)
                    fe
                case valid: Success[t] => valid
            }
            trial.toOption
        })
        System.out.println(transformed)
        System.out.println(accumulatorV2)
        0
    }

}
