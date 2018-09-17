package de.hpi.isg.dataprep.implementation

import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.model.error.{PreparationError, PropertyError}
import de.hpi.isg.dataprep.model.target.preparator.{Preparator, PreparatorImpl}
import de.hpi.isg.dataprep.preparators.AddProperty
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.{Consequences, ConversionHelper, SchemaUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  *
  * @author Lan Jiang
  * @since 2018/8/20
  */
class DefaultAddPropertyImpl extends PreparatorImpl {

    @throws(classOf[Exception])
    override protected def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        val preparator_ = getPreparatorInstance(preparator, classOf[AddProperty])
        val errorAccumulator = this.createErrorAccumulator(dataFrame)
        executeLogic(preparator_, dataFrame, errorAccumulator)
    }

    protected def executeLogic(preparator: AddProperty, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val targetPropertyName = preparator.targetPropertyName
        val targetType = preparator.targetType
        val positionInSchema = preparator.position
        val filling = Option{preparator.filling}

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
            }
        }
        new Consequences(resultDataFrame, errorAccumulator)
    }
}
