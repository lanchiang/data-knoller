package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.{DatasetUtil, ExecutionContext}
import de.hpi.isg.dataprep.preparators.define.MapNGram
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
class DefaultMapNGramImpl extends PreparatorImpl {

    //    override protected def executePreparator(preparator: AbstractPreparator, dataFrame: Dataset[Row]): ExecutionContext = {
    //        val preparator_ = this.getPreparatorInstance(preparator, classOf[MapNGram])
    //        val errorAccumulator = this.createErrorAccumulator(dataFrame)
    //
    //        val propertyName = preparator_.propertyName
    //        val n = preparator_.n
    //
    //        val dataType = dataFrame.schema(propertyName).dataType
    //
    //        val intermediate = dataFrame.withColumn(propertyName+"_ngram", lit(""))
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
    //
    //        createdDataset.count()
    //
    //        new ExecutionContext(createdDataset, errorAccumulator)
    //    }
    override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
        val preparator_ = abstractPreparator.asInstanceOf[MapNGram]

        val propertyName = preparator_.propertyName
        val n = preparator_.n

        val dataType = dataFrame.schema(propertyName).dataType

        val intermediate = dataFrame.withColumn(propertyName + "_ngram", lit(""))
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
