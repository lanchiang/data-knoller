package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.exceptions.{ParameterNotSpecifiedException, PreparationHasErrorException}
import de.hpi.isg.dataprep.model.error.{PreparationError, PropertyError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.AddProperty
import de.hpi.isg.dataprep.schema.SchemaUtils
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  * This class includes the default logic implementation of the preparator AddProperty.
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
    val filling = Option {preparator.filling}

    val schema = dataFrame.columns

    if (schema.contains(targetPropertyName)) {
      val error = new PropertyError(targetPropertyName, new PreparationHasErrorException("ColumnMetadata already exists."))
      errorAccumulator.add(error)
    }

    if (!SchemaUtils.positionIsInTheRange(dataFrame, positionInSchema)) {
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

  override def findMissingParametersImpl(preparator: AbstractPreparator, dataset: Dataset[Row]): Unit = {
    val cast = preparator.isInstanceOf[AddProperty] match {
      case true => preparator.asInstanceOf[AddProperty]
      case false => throw new RuntimeException(new ClassCastException("Preparator class cannot be cast"))
    }

    Option{cast.targetPropertyName} match {
      case None => throw new RuntimeException(new ParameterNotSpecifiedException("Target property name is not specified."))
      case Some(_) => {
        cast.targetType = Option{cast.targetType} match {
          case None => DataType.PropertyType.STRING // by default use the string type
          case Some(dataType) => dataType
        }

        cast.position = Option{cast.position} match {
          case None => dataset.columns.length // to the end of the schema
          case Some(position) => position
        }

        cast.filling = Option{cast.filling} match {
          case None => findFillingSequence(dataset, cast.targetPropertyName)
          case Some(filling) => filling
        }
      }
    }
  }

  /**
    * Returns the filling sequence depending on the data type of the `targetPropertyName`.
    *
    * @param dataset is the dataset containing the target property
    * @param targetPropertyName is the name of the target property whose value should be derived.
    * @return the derived string sequence that represents the filling.
    */
  private def findFillingSequence(dataset: Dataset[Row], targetPropertyName: String): String = {
    // Todo: find the filling depending on the dataType
    ???
  }
}
