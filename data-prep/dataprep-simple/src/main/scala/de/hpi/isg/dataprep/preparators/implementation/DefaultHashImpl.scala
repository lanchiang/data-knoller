package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.{ConversionHelper, DatasetUtil, ExecutionContext}
import de.hpi.isg.dataprep.preparators.define.Hash
import de.hpi.isg.dataprep.schema.SchemaUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.CollectionAccumulator

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class DefaultHashImpl extends AbstractPreparatorImpl {

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator_ = abstractPreparator.asInstanceOf[Hash]

    val propertyName = preparator_.propertyName
    val hashAlgorithm = preparator_.hashAlgorithm

    val dataType = dataFrame.schema(propertyName).dataType

    val intermediate = dataFrame.withColumn(propertyName + "_" + hashAlgorithm.toString, lit(""))

    val schema = intermediate.schema
    val rowEncoder = RowEncoder(intermediate.schema)

    val createdDataset = intermediate.flatMap(row => {
      val index = DatasetUtil.getFieldIndexByPropertyNameSafe(row, propertyName)
      val operatedValue = DatasetUtil.getValueByFieldIndex(row, index, dataType)
      val newVal = ConversionHelper.hash(operatedValue.toString, hashAlgorithm)

      val tryConvert = SchemaUtils.createRow(row, row.length, newVal)

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

    createdDataset.foreach(row => row)

    new ExecutionContext(createdDataset, errorAccumulator)
  }
}
