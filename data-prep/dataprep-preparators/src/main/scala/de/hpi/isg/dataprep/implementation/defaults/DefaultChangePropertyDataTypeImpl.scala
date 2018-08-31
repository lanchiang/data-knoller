package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.{Consequences, ConversionHelper}
import de.hpi.isg.dataprep.SparkPreparators.spark
import de.hpi.isg.dataprep.implementation.ChangePropertyDataTypeImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.prepmetadata.MetadataUtil
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType
import de.hpi.isg.dataprep.util.DataType.PropertyType
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
      * @param dataFrame  the operated [[Dataset<Row>]] instance.
      * @return an instance of [[Consequences]] that stores all the execution information.
      */
    override protected def executeLogic(preparator: ChangePropertyDataType, dataFrame: Dataset[Row],
                                        errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val targetDataType = preparator.getTargetType
        val fieldName = preparator.getPropertyName
        val sourceDatePattern = preparator.getSourceDatePattern
        val targetDatePattern = preparator.getTargetDatePattern
        // Here the program needs to check the existence of these fields.

        val createdRDD = dataFrame.rdd.flatMap(row => {
            val tryRow = Try {
                val tryConvert = targetDataType match {
                    case PropertyType.INTEGER => row.getAs[String](fieldName).toInt
                    case PropertyType.STRING => row.getAs[String](fieldName).toString
                    case PropertyType.DOUBLE => row.getAs[String](fieldName).toDouble
                    case PropertyType.DATE => {
                        ConversionHelper.toDate(row.getAs[String](fieldName),
                            sourceDatePattern, targetDatePattern)
                    }
                }
                row
            }
            val trial = tryRow match {
                case Failure(content) => {
                    errorAccumulator.add(new RecordError(row.getAs[String](fieldName), content))
                    tryRow
                }
                case Success(content) => {
                    tryRow
                }
            }
            trial.toOption
        })

        // persist
//        createdRDD.persist()

//        createdRDD.count()

        val resultDataFrame = spark.createDataFrame(createdRDD, dataFrame.schema)

        resultDataFrame.show()

        new Consequences(resultDataFrame, errorAccumulator)
        //        new Consequences(dataFrame, errorAccumulator)
    }
}
