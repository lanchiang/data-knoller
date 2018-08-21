package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.{Consequences, ConversionHelper}
import de.hpi.isg.dataprep.SparkPreparators.spark
import de.hpi.isg.dataprep.implementation.ChangePropertyDataTypeImpl
import de.hpi.isg.dataprep.model.metadata.MetadataUtil
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType
import de.hpi.isg.dataprep.util.PropertyDataType.PropertyType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  * @author Lan Jiang
  * @since 2018/8/19
  */
class DefaultChangePropertyDataTypeImpl extends ChangePropertyDataTypeImpl {
    /**
      * The subclass decides which specific scala code snippet to invoke.
      *
      * @param preparator the specific [[Preparator]] invoked.
      * @param dataFrame the operated [[Dataset<Row>]] instance.
      * @return an instance of [[Consequences]] that stores all the execution information.
      */
    override def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        val errorAccumulator = new CollectionAccumulator[(Any, Throwable)]
        dataFrame.sparkSession.sparkContext.register(errorAccumulator, "The error accumulator for preparator: Change property data type.")

        val preparator_ = super.getPreparatorInstance(preparator, classOf[ChangePropertyDataType])

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
}
