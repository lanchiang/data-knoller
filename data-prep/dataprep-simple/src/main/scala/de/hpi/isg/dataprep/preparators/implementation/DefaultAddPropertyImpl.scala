package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl

import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.model.error.{PreparationError, PropertyError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.define.AddProperty
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext, SchemaUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  *
  * @author Lan Jiang
  * @since 2018/8/20
  */
class DefaultAddPropertyImpl extends AbstractPreparatorImpl {

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[AddProperty]
    val targetPropertyName = preparator.targetPropertyName
    val targetType = preparator.targetType
    val positionInSchema = preparator.position
    val filling = Option {
      preparator.filling
    }

    val schema = dataFrame.columns

    if (schema.contains(targetPropertyName)) {
      val error = new PropertyError(targetPropertyName, new PreparationHasErrorException("ColumnMetadata already exists."))
      errorAccumulator.add(error)
    }

    if (!SchemaUtils.positionValidation(positionInSchema, schema)) {
      errorAccumulator.add(new PropertyError(targetPropertyName,
        new PreparationHasErrorException(String.format("Position %d is out of bound.", positionInSchema: Integer))))
    }

    var resultDataFrame = dataFrame

    if (errorAccumulator.isZero) {
      val value = filling.get
      resultDataFrame = targetType match {
        case PropertyType.INTEGER => {
          SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName,
            lit(value.toString.toInt)), positionInSchema)
        }
        case PropertyType.DOUBLE => {
          SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName,
            lit(value.toString.toDouble)), positionInSchema)
        }
        case PropertyType.STRING => {
          SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName,
            lit(value.toString)), positionInSchema)
        }
        case PropertyType.DATE => {
          val col = lit(ConversionHelper.getDefaultDate())
          SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName, col.cast(DateType)), positionInSchema)
        }
          // now the default is not working, just return the same dataFrame
        case _ => {
          dataFrame
        }
      }
    }
    new ExecutionContext(resultDataFrame, errorAccumulator)
  }
}
