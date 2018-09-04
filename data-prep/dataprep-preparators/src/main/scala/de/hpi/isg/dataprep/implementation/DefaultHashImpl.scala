package de.hpi.isg.dataprep.implementation

import de.hpi.isg.dataprep.model.error.RecordError
import de.hpi.isg.dataprep.{Consequences, ConversionHelper, DatasetUtil}
import de.hpi.isg.dataprep.model.target.preparator.{Preparator, PreparatorImpl}
import de.hpi.isg.dataprep.preparators.Hash
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.lit

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class DefaultHashImpl extends PreparatorImpl {

    @throws(classOf[Exception])
    override protected def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        val preparator_ = this.getPreparatorInstance(preparator, classOf[Hash])
        val errorAccumulator = this.createErrorAccumulator(dataFrame)

        val propertyName = preparator_.propertyName
        val hashAlgorithm = preparator_.hashAlgorithm

        val dataType = dataFrame.schema(propertyName).dataType

        val intermediate = dataFrame.withColumn(propertyName+"_"+hashAlgorithm.toString, lit(""))

        val rowEncoder = RowEncoder(intermediate.schema)

        val createdDataset = intermediate.flatMap(row => {
            val index = DatasetUtil.getFieldIndexByPropertyNameSafe(row, propertyName)
            val operatedValue = DatasetUtil.getValueByFieldIndex(row, index, dataType)

            val seq = row.toSeq
            val forepart = seq.take(row.length-1)

            val tryConvert = Try{
                val newSeq = (forepart :+ ConversionHelper.hash(operatedValue.toString, hashAlgorithm))
                val newRow = Row.fromSeq(newSeq)
                newRow
            }

            val convertOption = tryConvert match {
                case Failure(content) => {
                    errorAccumulator.add(new RecordError(operatedValue.toString, content))
                    tryConvert
                }
                case Success(content) => tryConvert
            }

            convertOption.toOption
        })(rowEncoder)

        createdDataset.persist()

        createdDataset.count()

        new Consequences(createdDataset, errorAccumulator)
    }
}
