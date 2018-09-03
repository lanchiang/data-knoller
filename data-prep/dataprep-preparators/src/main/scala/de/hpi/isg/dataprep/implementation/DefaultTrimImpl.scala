package de.hpi.isg.dataprep.implementation

import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.preparator.{Preparator, PreparatorImpl}
import de.hpi.isg.dataprep.preparators.Trim
import de.hpi.isg.dataprep.{Consequences, ConversionHelper}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/29
  */
class DefaultTrimImpl extends PreparatorImpl {

    override protected def executePreparator(preparator: Preparator, dataFrame: DataFrame): Consequences = {
        val preparator_ = this.getPreparatorInstance(preparator, classOf[Trim])
        val errorAccumulator = this.createErrorAccumulator(dataFrame)
        executeLogic(preparator_, dataFrame, errorAccumulator)
    }

    private def executeLogic(preparator: Trim, dataFrame: DataFrame, errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val propertyName = preparator.propertyName

        val rowEncoder = RowEncoder(dataFrame.schema)

        val createdDataset = dataFrame.flatMap(row => {
            val operatedValue = row.getAs[String](propertyName)
            val indexTry = Try{row.fieldIndex(propertyName)}
            val index = indexTry match {
                case Failure(content) => {
                    throw content
                }
                case Success(content) => {
                    content
                }
            }

            val seq = row.toSeq
            val forepart = seq.take(index)
            val backpart = seq.takeRight(row.length-index-1)

            val tryConvert = Try{
                val newSeq = (forepart :+ ConversionHelper.trim(operatedValue)) ++ backpart
                val newRow = Row.fromSeq(newSeq)
                newRow
            }

            val convertOption = tryConvert match {
                case Failure(content) => {
                    errorAccumulator.add(new RecordError(operatedValue, content))
                    tryConvert
                }
                case Success(content) => tryConvert
            }

            convertOption.toOption
        })(rowEncoder)
        createdDataset.count()

        new Consequences(createdDataset, errorAccumulator)
    }
}
