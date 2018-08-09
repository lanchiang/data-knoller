package de.hpi.isg.dataprep.spark

import de.hpi.isg.dataprep.Consequences
import de.hpi.isg.dataprep.SparkPreparators.spark
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType.PropertyType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}
import spark.implicits._

/**
  * @author Lan Jiang
  * @since 2018/8/7
  */
object ChangePropertyDataTypeImpl {

    def changePropertyDataType(dataFrame: DataFrame, columnName: String, sourceDataType: PropertyType, targetDataType: PropertyType): Consequences = {
        new Consequences(new CollectionAccumulator[(Any, Throwable)])
    }

    def changePropertyDataType(dataFrame: DataFrame, fieldName: String, targetDataType: PropertyType): Consequences = {
        val errorsAccumulator = new CollectionAccumulator[(Any, Throwable)]
        dataFrame.sparkSession.sparkContext.register(errorsAccumulator, "test")

        val transformed = dataFrame.rdd.flatMap(row => {
            val fe = Try {
                targetDataType match {
                    case PropertyType.INTEGER => row.getAs[String](fieldName).toInt
                    case PropertyType.STRING => row.getAs[String](fieldName).toString
                }
            }
            val trial = fe match {
                case Failure(t) => {
                    errorsAccumulator.add(row.getAs[String](fieldName), t)
                    fe
                }
                case valid: Success[t] => {
                    valid
                }
            }
            trial.toOption
        }).count() // count to update the accumulator
        System.out.println(errorsAccumulator.value)

        new Consequences(errorsAccumulator)
    }
}
