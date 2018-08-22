package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.{Consequences, SchemaUtils}
import de.hpi.isg.dataprep.implementation.MovePropertyImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, PropertyError}
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.preparators.MoveProperty
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  *
  * @author Lan Jiang
  * @since 2018/8/21
  */
class DefaultMovePropertyImpl extends MovePropertyImpl {

    override protected def executeLogic(preparator: MoveProperty, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val propertyName = preparator.getTargetPropertyName
        val newPosition = preparator.getNewPosition

        val schema = dataFrame.columns

        if (!SchemaUtils.positionValidation(newPosition, schema)) {
            errorAccumulator.add(new PropertyError(propertyName,
                new PreparationHasErrorException(String.format("New position %d in the schema is out of bound.", newPosition : Integer))))
        }

        val currentPosition = dataFrame.schema.fieldIndex(propertyName)

        var fore = schema.take(currentPosition)
        var back = schema.drop(currentPosition+1)
        val intermediate = (fore ++ back).clone()

        fore = intermediate.take(newPosition)
        back = intermediate.drop(newPosition)
        val newSchema = (fore :+ schema(currentPosition)) ++: back
        val columns = newSchema.map(col => dataFrame.col(col))

        val resultDataFrame = dataFrame.select(columns: _*)

        new Consequences(resultDataFrame, errorAccumulator)
    }
}
