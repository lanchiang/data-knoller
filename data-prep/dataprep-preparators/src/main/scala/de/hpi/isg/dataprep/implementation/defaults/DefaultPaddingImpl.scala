package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.{Consequences, ConversionHelper}
import de.hpi.isg.dataprep.implementation.PaddingImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.preparators.Padding
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/29
  */
class DefaultPaddingImpl extends PaddingImpl {

    override protected def executeLogic(preparator: Padding, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val propertyName = preparator.getPropertyName
        val expectedLength = preparator.getExpectedLength
        val padder = preparator.getPadder

        val createdRDD = dataFrame.rdd.flatMap(row => {
            val operatedValue = row.getAs[String](propertyName)
            val indexTry = Try{
                row.fieldIndex(propertyName)
            }
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
                val newSeq = (forepart :+ ConversionHelper.padding(operatedValue, expectedLength, padder)) ++ backpart
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
        })
        createdRDD.count()

        val resultDataFrame = dataFrame.sparkSession.createDataFrame(createdRDD, dataFrame.schema)
        new Consequences(resultDataFrame, errorAccumulator)
    }
}
