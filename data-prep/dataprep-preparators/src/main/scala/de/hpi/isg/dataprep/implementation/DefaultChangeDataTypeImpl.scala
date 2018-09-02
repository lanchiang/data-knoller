package de.hpi.isg.dataprep.implementation

import de.hpi.isg.dataprep.SparkPreparators.spark
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.preparator.{Preparator, PreparatorImpl}
import de.hpi.isg.dataprep.preparators.ChangeDataType
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.{Consequences, ConversionHelper}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  * @author Lan Jiang
  * @since 2018/8/19
  */
class DefaultChangeDataTypeImpl extends PreparatorImpl {

    @throws(classOf[Exception])
    override protected def executePreparator(preparator: Preparator, dataFrame: DataFrame): Consequences = {
        val preparator_ = getPreparatorInstance(preparator, classOf[ChangeDataType])
        val errorAccumulator = this.createErrorAccumulator( dataFrame)
        executeLogic(preparator_, dataFrame, errorAccumulator)
    }

    /**
      * The subclass decides which specific scala code snippet to invoke.
      *
      * @param preparator the specific [[Preparator]] invoked.
      * @param dataFrame  the operated [[Dataset<Row>]] instance.
      * @return an instance of [[Consequences]] that stores all the execution information.
      */
    protected def executeLogic(preparator: ChangeDataType, dataFrame: Dataset[Row],
                                        errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val fieldName = preparator.propertyName
        val targetDataType = preparator.targetType
        val sourceDatePattern = preparator.sourceDatePattern
        val targetDatePattern = preparator.targetDatePattern
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

        createdRDD.count()

        val resultDataFrame = spark.createDataFrame(createdRDD, dataFrame.schema)
        new Consequences(resultDataFrame, errorAccumulator)
    }
}