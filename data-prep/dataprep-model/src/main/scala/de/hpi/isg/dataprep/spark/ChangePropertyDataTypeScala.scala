package de.hpi.isg.dataprep.spark

import de.hpi.isg.dataprep.Consequences
import de.hpi.isg.dataprep.SparkPreparators.spark
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType.PropertyType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}
import spark.implicits._

/**
  * @author Lan Jiang
  * @since 2018/8/7
  */
object ChangePropertyDataTypeScala {

    def changePropertyDataType(dataFrame: DataFrame, fieldName: String, sourceDataType: PropertyType, targetDataType: PropertyType): Consequences = {
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

    def changePropertyDataType(dataFrame: DataFrame, _preparator: Preparator): Consequences = {
        val errorAccumulator = new CollectionAccumulator[(Any, Throwable)]
        dataFrame.sparkSession.sparkContext.register(errorAccumulator, "The error accumulator for preparator: Change property data type.")

        val preparator = _preparator.asInstanceOf[ChangePropertyDataType]
        val targetDataType = preparator.getTargetType
        val fieldName = preparator.getPropertyName
        /**
          * Here need to check the existence of these fields.
          */

        val transformed = dataFrame.rdd.flatMap(row => {
            val tryConvert = Try{
                targetDataType match {
                    case PropertyType.INTEGER => row.getAs[String](fieldName).toInt
                    case PropertyType.STRING => row.getAs[String](fieldName).toString
                    case PropertyType.DATE =>
                }
            }
            val trial = tryConvert match {
                case Failure(content) => {
                    errorAccumulator.add(row.getAs[String](fieldName), content)
                    tryConvert
                }
                case valid : Success[content] => valid
            }
            trial.toOption
        }).count()

        new Consequences(errorAccumulator)
    }

    @throws(classOf[Exception])
    def toDate(value: String, datePattern: String): DateType = {
        
    }
}
