package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangeDateFormat
import de.hpi.isg.dataprep.schema.SchemaUtils
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DataType.PropertyType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

class DefaultChangeDateFormatImpl extends PreparatorImpl {
    /**
      * The abstract class of preparator implementation.
      * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the
      *                           corresponding subclass in the implementation body.
      * @param dataFrame contains the intermediate dataset
      * @param errorAccumulator is the { @link CollectionAccumulator} to store preparation errors while executing the
      *                         preparator.
      * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
      * @throws Exception
      */
    override protected def executeLogic(
        abstractPreparator: AbstractPreparator,
        dataFrame: Dataset[Row],
        errorAccumulator: CollectionAccumulator[PreparationError]
    ): ExecutionContext = {
        val preparator = abstractPreparator.asInstanceOf[ChangeDateFormat]
        val fieldName = preparator.propertyName
        // Here the program needs to check the existence of these fields.

        val rowEncoder = RowEncoder(SchemaUtils.updateSchema(
            dataFrame.schema,
            fieldName,
            DataType.getSparkTypeFromInnerType(PropertyType.STRING)))

        val createdDataset = dataFrame.flatMap(row => {
            val index = row.fieldIndex(fieldName)
            val seq = row.toSeq
            val forepart = seq.take(index)
            val backpart = seq.takeRight(row.length - index - 1)

            val tryRow = Try {
                val targetPattern = preparator.targetDatePattern.get
                val tryConvert = if (preparator.sourceDatePattern.isDefined) {
                    val sourcePattern = preparator.sourceDatePattern.get
                    ConversionHelper.toDate(row.getAs[String](fieldName), sourcePattern, targetPattern)
                } else {
                    ConversionHelper.toDate(row.getAs[String](fieldName), targetPattern)
                }

                val newSeq = (forepart :+ tryConvert) ++ backpart
                val newRow = Row.fromSeq(newSeq)
                newRow
            }
            val trial = tryRow match {
                case Failure(content) =>
                    errorAccumulator.add(new RecordError(row.getAs[String](fieldName), content))
                    tryRow
                case Success(_) => tryRow
            }
            trial.toOption
        })(rowEncoder)

        // persist
        createdDataset.persist()

        createdDataset.count()

        new ExecutionContext(createdDataset, errorAccumulator)
    }
}
