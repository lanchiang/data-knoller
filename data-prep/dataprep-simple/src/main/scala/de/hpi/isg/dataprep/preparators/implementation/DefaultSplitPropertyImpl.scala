package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

class DefaultSplitPropertyImpl extends PreparatorImpl {

  override def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[SplitProperty]
    val propertyName = preparator.propertyName
    val separator = preparator.separator
    val fromLeft = preparator.fromLeft
    val times = preparator.times

    var resultDataFrame = dataFrame

    for (i <- 0 to times) {
      resultDataFrame = resultDataFrame.withColumn(s"split $i", lit(""))
    }

    new ExecutionContext(resultDataFrame, errorAccumulator)

    val rowEncoder = RowEncoder(resultDataFrame.schema)

    val createdDataset = dataFrame.flatMap(row => {
      val indexTry = Try {
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
      val operatedValue = row.getAs[String](index)

      val splittedValues = operatedValue.split(separator, times+1)
      val oldRow = row.toSeq
      val fill = List.fill(times+1 - splittedValues.length)("").toSeq

      val tryConvert = Try {
        val newSeq = oldRow ++ splittedValues ++ fill
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

    new ExecutionContext(createdDataset, errorAccumulator)
  }
}
