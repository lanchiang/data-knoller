package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl

import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.{DatasetUtil, ExecutionContext}
import de.hpi.isg.dataprep.preparators.define.GenerateUnicode
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class DefaultGenerateUnicodeImpl extends AbstractPreparatorImpl {

  //    override protected def executePreparator(preparator: AbstractPreparator, dataFrame: Dataset[Row]): ExecutionContext = {
  //        val preparator_ = getPreparatorInstance(preparator, classOf[GenerateUnicode])
  //        val errorAccumulator = createErrorAccumulator(dataFrame)
  //
  //        val propertyName = preparator_.propertyName
  //
  //        val dataType = dataFrame.schema(propertyName).dataType
  //
  //        val intermediate = dataFrame.withColumn(propertyName+"_unicode", lit(""))
  //        val rowEncoder = RowEncoder(intermediate.schema)
  //
  //        val createdDataset = intermediate.flatMap(row => {
  //            val index = DatasetUtil.getFieldIndexByPropertyNameSafe(row, propertyName)
  //            val operatedValue = DatasetUtil.getValueByFieldIndex(row, index, dataType)
  //
  //            val seq = row.toSeq
  //            val forepart = seq.take(row.length-1)
  //
  //            val tryConvert = Try{
  //                val newSeq = forepart :+ None
  //                val newRow = Row.fromSeq(newSeq)
  //                newRow
  //            }
  //
  //            val convertOption = tryConvert match {
  //                case Failure(content) => {
  //                    errorAccumulator.add(new RecordError(operatedValue.toString, content))
  //                    tryConvert
  //                }
  //                case Success(content) => tryConvert
  //            }
  //
  //            convertOption.toOption
  //        })(rowEncoder)
  //
  //        createdDataset.persist()
  //        createdDataset.count()
  //
  //        new ExecutionContext(createdDataset, errorAccumulator)
  //    }

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[GenerateUnicode]

    val propertyName = preparator.propertyName

    val dataType = dataFrame.schema(propertyName).dataType

    val intermediate = dataFrame.withColumn(propertyName + "_unicode", lit(""))
    val rowEncoder = RowEncoder(intermediate.schema)

    val createdDataset = intermediate.flatMap(row => {
      val index = DatasetUtil.getFieldIndexByPropertyNameSafe(row, propertyName)
      val operatedValue = DatasetUtil.getValueByFieldIndex(row, index, dataType)

      val seq = row.toSeq
      val forepart = seq.take(row.length - 1)

      val tryConvert = Try {
        val newSeq = forepart :+ None
        val newRow = Row.fromSeq(newSeq)
        newRow
      }

      val convertOption = tryConvert match {
        case Failure(content) => {
          errorAccumulator.add(new RecordError(operatedValue.toString, content))
          tryConvert
        }
        case Success(content) => tryConvert
      }

      convertOption.toOption
    })(rowEncoder)

    createdDataset.persist()
    createdDataset.count()

    new ExecutionContext(createdDataset, errorAccumulator)
  }
}
