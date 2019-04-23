package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.Collapse
import de.hpi.isg.dataprep.schema.SchemaUtils
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/28
  */
class DefaultCollapseImpl extends AbstractPreparatorImpl {

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[Collapse]
    val propertyName = preparator.propertyName

    val rowEncoder = RowEncoder(dataFrame.schema)

    val resultDF = dataFrame.flatMap(row => {
      val newVal = ConversionHelper.collapse(row.getAs[String](propertyName))
      val index = row.fieldIndex(propertyName)

      val tryConvert = SchemaUtils.createRow(row, index, newVal)

      val trial = tryConvert match {
        case Failure(content) => {
          errorAccumulator.add(new RecordError(row.getAs[String](propertyName), content))
          tryConvert
        }
        case Success(content) => {
          tryConvert
        }
      }
      trial.toOption
    })(rowEncoder)

    resultDF.count()

    new ExecutionContext(resultDF, errorAccumulator)
  }

  /**
    * Currently no need to implement this function because this preparator always that a valid value for the propertyName parameter.
    */
  override def findMissingParametersImpl(preparator: AbstractPreparator, dataset: Dataset[Row]): Unit = {
    ???
  }
}
