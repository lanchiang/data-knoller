package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.metadata.PhoneNumberFormat
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangePhoneFormat
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

class DefaultChangePhoneFormatImpl extends AbstractPreparatorImpl with Serializable {
  /**
    * The abstract class of preparator implementation.
    *
    * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
    * @param dataFrame          contains the intermediate dataset
    * @param errorAccumulator   is the { @link CollectionAccumulator} to store preparation errors while executing the preparator.
    * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
    * @throws Exception
    */
  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[ChangePhoneFormat]

    val createdDataset = dataFrame.flatMap { row =>
      val index = row.fieldIndex(preparator.propertyName)
      val operatedValue = row.getAs[String](index)
      val (start, end) = row.toSeq.splitAt(index)

      val convertTry = Option(preparator.sourceFormat)
        .fold(convert(operatedValue, preparator.targetFormat))(convert(operatedValue, _, preparator.targetFormat))
        .flatMap(number => Try(Row.fromSeq(start ++ Seq(number) ++ end)))

      convertTry match {
        case Failure(exception) => errorAccumulator.add(new RecordError(operatedValue, exception))
        case Success(_) => ()
      }

      convertTry.toOption

    }(RowEncoder(dataFrame.schema))

    createdDataset.persist()

    new ExecutionContext(createdDataset, errorAccumulator)
  }

  /**
    * Converting a given phone number
    * @param phoneNumber
    * @param sourceFormat
    * @param targetFormat
    * @return
    */
  private def convert(phoneNumber: String, sourceFormat: PhoneNumberFormat, targetFormat: PhoneNumberFormat): Try[String] = {
    NormalizedPhoneNumber.fromMeta(sourceFormat)(phoneNumber) flatMap NormalizedPhoneNumber.toMeta(targetFormat)
  }

  /**
    * Converting a given phone number
    * @param phoneNumber
    * @param targetFormat
    * @return
    */
  private def convert(phoneNumber: String, targetFormat: PhoneNumberFormat): Try[String] = ???
}
