package de.hpi.isg.dataprep.implementation.defaults

import java.util.Date

import de.hpi.isg.dataprep.{Consequences, ConversionHelper}
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.implementation.AddPropertyImpl
import de.hpi.isg.dataprep.model.target.preparator.Preparator
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

        val preparator_ = super.getPreparatorInstance(preparator)

        val targetPropertyName = preparator_.getTargetPropertyName
        val targetPropertyDataType = preparator_.getTargetPropertyDataType
        val positionInSchema = preparator_.getPositionInSchema
        val defaultValue = Option{preparator_.getDefaultValue}

        val schema = dataFrame.columns

        if (schema.contains(targetPropertyName)) {
            errorAccumulator.add(targetPropertyName, new PreparationHasErrorException("Property already exists."))
        }

        if (!positionValidation(positionInSchema, schema)) {
            errorAccumulator.add(positionInSchema, new PreparationHasErrorException("Position of the new property in the schema is out of bound."))
        }

        val resultDataFrame = (targetPropertyDataType, defaultValue) match {
            case (PropertyType.INTEGER, Some(content)) => changePropertyPosition(dataFrame.withColumn(targetPropertyName,
                lit(content.toString.toInt)), positionInSchema)
            case (PropertyType.INTEGER, None) => changePropertyPosition(dataFrame.withColumn(targetPropertyName,
                lit(0)), positionInSchema)
            case (PropertyType.DOUBLE, Some(content)) => changePropertyPosition(dataFrame.withColumn(targetPropertyName,
                lit(content.toString.toDouble)), positionInSchema)
            case (PropertyType.DOUBLE, None) => changePropertyPosition(dataFrame.withColumn(targetPropertyName,
                lit(0.0)), positionInSchema)
            case (PropertyType.STRING, Some(content)) => changePropertyPosition(dataFrame.withColumn(targetPropertyName,
                lit(content.toString)), positionInSchema)
            case (PropertyType.STRING, None) => changePropertyPosition(dataFrame.withColumn(targetPropertyName,
                lit("")), positionInSchema)
            case (PropertyType.DATE, Some(content)) => changePropertyPosition(dataFrame.withColumn(targetPropertyName,
                null), positionInSchema)
            case (PropertyType.DATE, None) => {
                val col = lit(ConversionHelper.getDefaultDate())
                changePropertyPosition(dataFrame.withColumn(targetPropertyName, col.cast(DateType)), positionInSchema)
            }
        }
        resultDataFrame.show()

        new Consequences(resultDataFrame, errorAccumulator)
    }

    val changePropertyPosition = (dataFrame: DataFrame, position: Int) => {
        val columns = dataFrame.columns

        val headPart = columns.slice(0, position)
        val tailPart = columns.slice(position, columns.length - 1)
        val reorderedSchema = headPart ++ columns.slice(columns.length - 1, columns.length) ++ tailPart

        dataFrame.select(reorderedSchema.map(column => col(column)): _*)
    }

    private def positionValidation(positionInSchema : Int, schema : Array[String]): Boolean = {
        if (positionInSchema < 0 || positionInSchema > schema.length) {
            false
        } else {
            true
        }
    }
}
