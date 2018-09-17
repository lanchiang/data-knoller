package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.preparator.{AbstractPreparator, PreparatorImpl}
import de.hpi.isg.dataprep.preparators.define.Collapse
import de.hpi.isg.dataprep.{Consequences, ConversionHelper}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/28
  */
class DefaultCollapseImpl extends PreparatorImpl {

    @throws(classOf[Exception])
    override protected def executePreparator(preparator: AbstractPreparator, dataFrame: DataFrame): Consequences = {
        val preparator_ = this.getPreparatorInstance(preparator, classOf[Collapse])
        val errorAccumulator = this.createErrorAccumulator(dataFrame)
        executeLogic(preparator_, dataFrame, errorAccumulator)
    }

    protected def executeLogic(preparator: Collapse, dataFrame: DataFrame,
                               errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val propertyName = preparator.propertyName

        val rowEncoder = RowEncoder(dataFrame.schema)

        val resultDF = dataFrame.flatMap(row => {
            val index = row.fieldIndex(propertyName)
            val seq = row.toSeq
            val forepart = seq.take(index)
            val backpart = seq.takeRight(row.length - index - 1)

            val tryConvert = Try{
                val newSeq = (forepart :+ ConversionHelper.collapse(row.getAs[String](propertyName))) ++ backpart
                val newRow = Row.fromSeq(newSeq)
                newRow
            }
            val trial = tryConvert match {
                case Failure(content) => {
                    errorAccumulator.add(new RecordError(row.getAs[String](propertyName), content))
                    tryConvert
                }
                case Success(content) => {
                    tryConvert
                }
            }
            trial.toOption
        })(rowEncoder)

        resultDF.count()

        new Consequences(resultDF, errorAccumulator)
    }
}
