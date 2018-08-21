package de.hpi.isg.dataprep.implementation.defaults

import java.util.Date

import de.hpi.isg.dataprep.{Consequences, ConversionHelper, SchemaUtils}
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.implementation.AddPropertyImpl
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.preparators.AddProperty
import de.hpi.isg.dataprep.util.PropertyDataType.PropertyType
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

    override protected def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        val errorAccumulator = new CollectionAccumulator[(Any, Throwable)]
        dataFrame.sparkSession.sparkContext.register(errorAccumulator, "The error accumulator for preparartor, Add Property.")

        val preparator_ = super.getPreparatorInstance(preparator, classOf[AddProperty])

        val targetPropertyName = preparator_.getTargetPropertyName
        val targetPropertyDataType = preparator_.getTargetPropertyDataType
        val positionInSchema = preparator_.getPositionInSchema
        val defaultValue = Option{preparator_.getDefaultValue}

        val schema = dataFrame.columns

        if (schema.contains(targetPropertyName)) {
            errorAccumulator.add(targetPropertyName, new PreparationHasErrorException("Property already exists."))
        }

        if (!SchemaUtils.positionValidation(positionInSchema, schema)) {
            errorAccumulator.add(positionInSchema, new PreparationHasErrorException("Position of the new property in the schema is out of bound."))
        }

        val resultDataFrame = (targetPropertyDataType, defaultValue) match {
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
//        resultDataFrame.show()

        new Consequences(resultDataFrame, errorAccumulator)
    }
}
