package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.metadata.{PhoneNumberFormat, PhoneNumberFormatComponentType}
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
    import TaggerInstances._

    val preparator = abstractPreparator.asInstanceOf[ChangePhoneFormat]
    val tagger = Option(preparator.sourceFormat)
      .map(_.components.map(_.componentType))
      .fold(phoneNumberTagger(PhoneNumberFormatComponentType.ordered))(phoneNumberTagger)

    val createdDataset = dataFrame.flatMap { row =>
      val index = row.fieldIndex(preparator.propertyName)
      val operatedValue = row.getAs[String](index)
      val (start, end) = row.toSeq.splitAt(index)
      val convertTry = convert(operatedValue, preparator.targetFormat)(tagger)
        .flatMap(phoneNumber => Try(Row.fromSeq(start ++ Seq(phoneNumber) ++ end)))

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
    * Converting a phone number to a target format
    * @param phoneNumber String containing the phone number
    * @param targetFormat Source format of the phone number
    * @return Try of the converted phone number
    */
  def convert(phoneNumber: String, targetFormat: PhoneNumberFormat)(implicit tagger: Tagger[PhoneNumberFormatComponentType]): Try[String] =
    PhoneNumber(phoneNumber).convert(targetFormat).map(_.toString)
}
