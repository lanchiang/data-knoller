package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.{ConversionHelper, DatasetUtil, ExecutionContext}
import de.hpi.isg.dataprep.preparators.define.Hash
import de.hpi.isg.dataprep.schema.SchemaUtils
import de.hpi.isg.dataprep.util.HashAlgorithm
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}

import scala.util.{Failure, Success}
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

  override def findMissingParametersImpl(preparator: AbstractPreparator, dataset: Dataset[Row]): Unit = {
    val cast = preparator match {
      case _ : Hash => preparator.asInstanceOf[Hash]
      case _ => throw new RuntimeException(new ClassCastException("Preparator class cannot be cast."))
    }

    Option{cast.propertyName} match {
      case None => throw new RuntimeException(new ParameterNotSpecifiedException("Target property name is not specified."))
      case Some(_) => {
        cast.hashAlgorithm = Option{cast.hashAlgorithm} match {
          case None => findHashAlgorithm(dataset, cast.propertyName) //Todo: if the hash algorithm is not specified, find it.
          case Some(algorithm) => algorithm
        }
      }
    }
  }

  private def findHashAlgorithm(dataset: Dataset[Row], propertyName: String): HashAlgorithm = {
    // by default return MD5
    Hash.DEFAULT_ALGORITHM
  }
}
