package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.define.RemoveCharacters
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/30
  */
class DefaultRemoveCharactersImpl extends AbstractPreparatorImpl {

  //    override protected def executePreparator(preparator: AbstractPreparator, dataFrame: Dataset[Row]): ExecutionContext = {
  //        val preparator_ = this.getPreparatorInstance(preparator, classOf[RemoveCharacters])
  //        val errorAccumulator = this.createErrorAccumulator(dataFrame)
  //        executeLogic(preparator_, dataFrame, errorAccumulator)
  //    }

  //    private def executeLogic(preparator: RemoveCharacters, dataFrame: DataFrame, errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
  //        val propertyName = preparator.propertyName
  //        val removeCharactersMode = preparator.mode
  //        val userSpecifiedCharacters = preparator.userSpecifiedCharacters
  //
  //        val rowEncoder = RowEncoder(dataFrame.schema)
  //
  //        val createdDataset = dataFrame.flatMap(row => {
  //            val indexTry = Try{row.fieldIndex(propertyName)}
  //            val index = indexTry match {
  //                case Failure(content) => throw content
  //                case Success(content) => content
  //            }
  //            val operatedValue = row.getAs[String](index)
  //
  //            val seq = row.toSeq
  //            val forepart = seq.take(index)
  //            val backpart = seq.takeRight(row.length-index-1)
  //
  //            val tryConvert = Try{
  //                val newSeq = (forepart :+ ConversionHelper.removeCharacters(operatedValue, removeCharactersMode, userSpecifiedCharacters)) ++ backpart
  //                val newRow = Row.fromSeq(newSeq)
  //                newRow
  //            }
  //
  //            val trial = tryConvert match {
  //                case Failure(content) => {
  //                    errorAccumulator.add(new RecordError(operatedValue, content))
  //                    tryConvert
  //                }
  //                case Success(content) => tryConvert
  //            }
  //            trial.toOption
  //        })(rowEncoder)
  //
  //        new ExecutionContext(createdDataset, errorAccumulator)
  //    }

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[RemoveCharacters]
    val propertyName = preparator.propertyName
    val removeCharactersMode = preparator.mode
    val userSpecifiedCharacters = preparator.userSpecifiedCharacters

    val rowEncoder = RowEncoder(dataFrame.schema)

    val createdDataset = dataFrame.flatMap(row => {
      val indexTry = Try {
        row.fieldIndex(propertyName)
      }
      val index = indexTry match {
        case Failure(content) => throw content
        case Success(content) => content
      }
      val operatedValue = row.getAs[String](index)

      val seq = row.toSeq
      val forepart = seq.take(index)
      val backpart = seq.takeRight(row.length - index - 1)

      val tryConvert = Try {
        val newSeq = (forepart :+ ConversionHelper.removeCharacters(operatedValue, removeCharactersMode, userSpecifiedCharacters)) ++ backpart
        val newRow = Row.fromSeq(newSeq)
        newRow
      }

      val trial = tryConvert match {
        case Failure(content) => {
          errorAccumulator.add(new RecordError(operatedValue, content))
          tryConvert
        }
        case Success(content) => tryConvert
      }
      trial.toOption
    })(rowEncoder)

    new ExecutionContext(createdDataset, errorAccumulator)
  }
}
