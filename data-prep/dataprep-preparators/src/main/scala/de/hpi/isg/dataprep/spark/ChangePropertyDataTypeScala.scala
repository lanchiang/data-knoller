package de.hpi.isg.dataprep.spark

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

    def changePropertyDataType(dataFrame: DataFrame, preparator: Preparator): Consequences = {
        val errorAccumulator = new CollectionAccumulator[(Any, Throwable)]
        dataFrame.sparkSession.sparkContext.register(errorAccumulator, "The error accumulator for preparator: Change property data type.")

        val preparator_ = preparator.asInstanceOf[ChangePropertyDataType]
        val targetDataType = preparator_.getTargetType
        val fieldName = preparator_.getPropertyName
        val sourceDatePattern = preparator_.getSourceDatePattern
        val targetDatePattern = preparator_.getTargetDatePattern
        val metadata = preparator_.getMetadataValue
        // Here the program needs to check the existence of these fields.

        val createdRDD = dataFrame.rdd.filter(row => {
            val tryConvert = Try {
                targetDataType match {
                    case PropertyType.INTEGER => row.getAs[String](fieldName).toInt
                    case PropertyType.STRING => row.getAs[String](fieldName).toString
                    case PropertyType.DOUBLE => row.getAs[String](fieldName).toDouble
                    case PropertyType.DATE => {
                        val originDatePattern = metadata.get(MetadataUtil.PROPERTY_DATATYPE)
                        ConversionHelper.toDate(row.getAs[String](fieldName),
                            sourceDatePattern, targetDatePattern)
                    }
                }
            }
            val trial = tryConvert match {
                case Failure(content) => {
                    errorAccumulator.add(row.getAs[String](fieldName), content)
                    false
                }
                case valid: Success[content] => {
                    true
                }
            }
            trial
        })
        createdRDD.count()
        val resultDataFrame = spark.createDataFrame(createdRDD, dataFrame.schema)

        new Consequences(resultDataFrame, errorAccumulator)
    }

    def rowSelection(dataFrame: DataFrame): Boolean = {
        true
    }
}
