package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.Padding
import de.hpi.isg.dataprep.schema.SchemaUtils
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/29
  */
class DefaultPaddingImpl extends AbstractPreparatorImpl {

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[Padding]
    val propertyName = preparator.propertyName
    val expectedLength = preparator.expectedLength
    val padder = preparator.padder

    val rowEncoder = RowEncoder(dataFrame.schema)

    val createdDataset = dataFrame.flatMap(row => {
      val index = row.fieldIndex(propertyName)
      val operatedValue = row.getAs[String](index)

      val tryCreateRow = Try{SchemaUtils.createOneRow(row, index, ConversionHelper.padding(operatedValue, expectedLength, padder))}

      val createRowOption = tryCreateRow match {
        case Failure(content) => {
          errorAccumulator.add(new RecordError(operatedValue, content))
          tryCreateRow
        }
        case Success(_) => tryCreateRow
      }
      createRowOption.toOption
    })(rowEncoder)

    createdDataset.foreach(row => row)

    new ExecutionContext(createdDataset, errorAccumulator)
  }
}
