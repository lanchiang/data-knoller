package de.hpi.isg.dataprep.preparators.implementation

import java.util

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.define.ChangeDataType
import de.hpi.isg.dataprep.schema.SchemaUtils
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  * @author Lan Jiang
  * @since 2018/8/19
  */
class DefaultChangeDataTypeImpl extends AbstractPreparatorImpl {

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[ChangeDataType]
    val fieldName = preparator.propertyName
    val targetDataType = preparator.targetType

    val metadataRepository = preparator.getPreparation.getPipeline.getMetadataRepository
    val metadata = new PropertyDataType(fieldName, null)
    val sourceDataType = Option(preparator.sourceType) match {
      case None => metadataRepository.getMetadata(metadata).asInstanceOf[PropertyDataType].getPropertyDataType
      case Some(_) => preparator.sourceType
    }

    val sourceDatePattern = preparator.sourceDatePattern
    val targetDatePattern = preparator.targetDatePattern
    // Here the program needs to check the existence of these fields.

    val rowEncoder = RowEncoder(SchemaUtils.updateSchema(dataFrame.schema, fieldName, DataType.getSparkTypeFromInnerType(targetDataType)))

    val createdDataset = dataFrame.flatMap(row => {
      val index = row.fieldIndex(fieldName)
      val seq = row.toSeq
      val forepart = seq.take(index)
      val backpart = seq.takeRight(row.length - index - 1)

      val tryRow = Try {
        val tryConvert = targetDataType match {
          case PropertyType.INTEGER => {
            sourceDataType match {
              case PropertyType.INTEGER => row.getAs[Int](fieldName)
              case PropertyType.STRING => row.getAs[String](fieldName).toInt
              case PropertyType.DOUBLE => row.getAs[Double](fieldName).toInt
              case PropertyType.DATE => {
                ConversionHelper.toDate(row.getAs[Int](fieldName).toString,
                  sourceDatePattern, targetDatePattern)
              }
              case _ => {
                val metadataInRepo = metadataRepository.getMetadata(metadata)

              }
            }
          }
          case PropertyType.STRING => {
            sourceDataType match {
              case PropertyType.INTEGER => row.getAs[Int](fieldName).toString
              case PropertyType.STRING => row.getAs[String](fieldName)
              case PropertyType.DOUBLE => row.getAs[Double](fieldName).toString
              case PropertyType.DATE => {
                ConversionHelper.toDate(row.getAs[String](fieldName).toString,
                  sourceDatePattern, targetDatePattern)
              }
                // default is not determined yet, simply return None
              case _ => {
                None
              }
            }
          }
          case PropertyType.DOUBLE => {
            sourceDataType match {
              case PropertyType.INTEGER => row.getAs[Int](fieldName).toDouble
              case PropertyType.STRING => row.getAs[String](fieldName).toDouble
              case PropertyType.DOUBLE => row.getAs[Double](fieldName)
              case PropertyType.DATE => {
                ConversionHelper.toDate(row.getAs[Double](fieldName).toString,
                  sourceDatePattern, targetDatePattern)
              }
                // default is not determined yet, simply return None
              case _ => {
                None
              }
            }
          }
          case PropertyType.DATE => {
            sourceDataType match {
              case PropertyType.INTEGER => row.getAs[String](fieldName).toInt
              case PropertyType.STRING => row.getAs[String](fieldName).toString
              case PropertyType.DOUBLE => row.getAs[String](fieldName).toDouble
              case PropertyType.DATE => {
                ConversionHelper.toDate(row.getAs[String](fieldName).toString,
                  sourceDatePattern, targetDatePattern)
              }
                // default is not determined yet, simply return None
              case _ => {
                None
              }
            }
          }
          case _ => {
            None
          }
        }

        val newSeq = (forepart :+ tryConvert) ++ backpart
        val newRow = Row.fromSeq(newSeq)
        newRow
      }
      val trial = tryRow match {
        case Failure(content) => {
          errorAccumulator.add(new RecordError(row.getAs[String](fieldName), content))
          tryRow
        }
        case Success(_) => {
          tryRow
        }
      }
      trial.toOption
    })(rowEncoder)

    // persist
    createdDataset.persist()

    createdDataset.count()

    new ExecutionContext(createdDataset, errorAccumulator)
  }
}