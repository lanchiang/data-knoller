package de.hpi.isg.dataprep.implementation.defaults

import java.util.Date

import de.hpi.isg.dataprep.{Consequences, ConversionHelper, SchemaUtils}
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.implementation.AddPropertyImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, PropertyError}
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.preparators.AddProperty
import de.hpi.isg.dataprep.util.DataType.PropertyType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.sql.functions.{col, lit, to_date, unix_timestamp}
import org.apache.spark.sql.types.{DateType, TimestampType}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/20
  */
class DefaultAddPropertyImpl extends AddPropertyImpl {

    override protected def executeLogic(preparator: AddProperty, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val targetPropertyName = preparator.getTargetPropertyName
        val targetPropertyDataType = preparator.getTargetPropertyDataType
        val positionInSchema = preparator.getPositionInSchema
        val defaultValue = Option{preparator.getDefaultValue}

        val schema = dataFrame.columns

        if (schema.contains(targetPropertyName)) {
            val error = new PropertyError(targetPropertyName, new PreparationHasErrorException("Property already exists."))
            errorAccumulator.add(error)
        }

        if (!SchemaUtils.positionValidation(positionInSchema, schema)) {
            errorAccumulator.add(new PropertyError(targetPropertyName,
                new PreparationHasErrorException(String.format("Position %d is out of bound.", positionInSchema: Integer))))
        }

        var resultDataFrame = dataFrame

        if (errorAccumulator.isZero) {
            resultDataFrame = (targetPropertyDataType, defaultValue) match {
                case (PropertyType.INTEGER, Some(content)) => SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName,
                    lit(content.toString.toInt)), positionInSchema)
                case (PropertyType.INTEGER, None) => SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName,
                    lit(0)), positionInSchema)
                case (PropertyType.DOUBLE, Some(content)) => SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName,
                    lit(content.toString.toDouble)), positionInSchema)
                case (PropertyType.DOUBLE, None) => SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName,
                    lit(0.0)), positionInSchema)
                case (PropertyType.STRING, Some(content)) => SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName,
                    lit(content.toString)), positionInSchema)
                case (PropertyType.STRING, None) => SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName,
                    lit("")), positionInSchema)
                case (PropertyType.DATE, Some(content)) => SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName,
                    null), positionInSchema)
                case (PropertyType.DATE, None) => {
                    val col = lit(ConversionHelper.getDefaultDate())
                    SchemaUtils.lastToNewPosition(dataFrame.withColumn(targetPropertyName, col.cast(DateType)), positionInSchema)
                }
            }
        }
        new Consequences(resultDataFrame, errorAccumulator)
    }
}
