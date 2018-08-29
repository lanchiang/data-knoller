package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.{Consequences, ConversionHelper}
import de.hpi.isg.dataprep.implementation.CollapseImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.preparators.Collapse
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/28
  */
class DefaultCollapseImpl extends CollapseImpl {

    override protected def executeLogic(preparator: Collapse, dataFrame: Dataset[Row],
                                        errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val propertyName = preparator.getPropertyName

        val createdRDD = dataFrame.rdd.flatMap(row => {
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
//            val newSeq = (forepart :+ trial.toOption.get) ++ backpart
//            val newRow = Row.fromSeq(newSeq)
//            newRow
        })
        createdRDD.count()

        val resultDataFrame = dataFrame.sparkSession.createDataFrame(createdRDD, dataFrame.schema)
        new Consequences(resultDataFrame, errorAccumulator)
    }
}
