package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException
import de.hpi.isg.dataprep.{Consequences, SchemaUtils}
import de.hpi.isg.dataprep.implementation.MovePropertyImpl
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

    override protected def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        val errorAccumulator = new CollectionAccumulator[(Any, Throwable)]
        dataFrame.sparkSession.sparkContext.register(errorAccumulator, "The error accumulator for preparator, Add Property.")

        val preparator_ = super.getPreparatorInstance(preparator, classOf[MoveProperty])

        val propertyName = preparator_.getTargetPropertyName
        val newPosition = preparator_.getNewPosition

        val schema = dataFrame.columns

        if (!SchemaUtils.positionValidation(newPosition, schema)) {
            errorAccumulator.add(newPosition, new PreparationHasErrorException("Position of the new property in the schema is out of bound."))
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
