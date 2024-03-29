package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ReplaceSubstring
import de.hpi.isg.dataprep.schema.SchemaUtils
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/29
  */
class DefaultReplaceSubstringImpl extends AbstractPreparatorImpl {

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: DataFrame,
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[ReplaceSubstring]
    val propertyName = preparator.propertyName
    val source = preparator.source
    val replacement = preparator.replacement
    val times = preparator.times // replace only the first N.

    val rowEncoder = RowEncoder(dataFrame.schema)

    val createdDataset = dataFrame.flatMap(row => {
      val index = row.fieldIndex(propertyName)
      val operatedValue = row.getAs[String](index)

      val tryConvert = SchemaUtils.createRow(row, index, ConversionHelper.replaceSubstring(operatedValue, source, replacement, times))
      val convertOption = tryConvert match {
        case Failure(content) => {
          errorAccumulator.add(new RecordError(operatedValue, content))
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
