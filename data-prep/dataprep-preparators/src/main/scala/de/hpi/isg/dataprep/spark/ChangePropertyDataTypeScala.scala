package de.hpi.isg.dataprep.spark

import java.text.SimpleDateFormat
import java.util.Date

import de.hpi.isg.dataprep.{Consequences, ConversionHelper}
import de.hpi.isg.dataprep.SparkPreparators.spark
import de.hpi.isg.dataprep.model.metadata.MetadataUtil
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType.PropertyType
import org.apache.spark.sql.DataFrame
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
        val datePattern = preparator.getDatePattern
        val metadata = preparator.getMetadataValue
        /**
          * Here need to check the existence of these fields.
          */

        val transformed = dataFrame.rdd.flatMap(row => {
            val tryConvert = Try{
                targetDataType match {
                    case PropertyType.INTEGER => row.getAs[String](fieldName).toInt
                    case PropertyType.STRING => row.getAs[String](fieldName).toString
                    case PropertyType.DATE => {
                        val originDatePattern = metadata.get(MetadataUtil.PROPERTY_DATATYPE)
                        ConversionHelper.toDate(row.getAs[String](fieldName), datePattern)
                    }
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
        })
        // executes an action to refresh the accumulator.
//        transformed.foreach(any => println(any))
        transformed.count()

        new Consequences(errorAccumulator)
    }
}
