package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.metadata.{DINPhoneNumberFormat, NANPPhoneNumberFormat, PhoneNumberFormat}
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangePhoneFormat
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row}
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
  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: DataFrame, errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    import TaggerInstances._

    val createdDataset = abstractPreparator match {
      case preparator: ChangePhoneFormat[NANPPhoneNumberFormat] =>
        val tagger = nanpTagger(Option(preparator.sourceFormat).map(_.components.map(_.componentType)))
        createDataset(preparator, errorAccumulator)(dataFrame)(tagger)
      case preparator: ChangePhoneFormat[DINPhoneNumberFormat] =>
        val tagger = dinTagger(Option(preparator.sourceFormat).map(_.components.map(_.componentType)))
        createDataset(preparator, errorAccumulator)(dataFrame)(tagger)
      case _ => dataFrame
    }

    createdDataset.persist()

    new ExecutionContext(createdDataset, errorAccumulator)
  }

  /**
    * Creating the converted dataset
    * @param preparator instance of an abstract preparator
    * @param errorAccumulator the error repository
    * @param dataFrame dataset that should be converted
    * @param tagger instance of a tagger
    * @return Dataset containing all successful converted entries
    */
  private def createDataset[A](preparator: ChangePhoneFormat[A], errorAccumulator: CollectionAccumulator[PreparationError])(dataFrame: DataFrame)(implicit tagger: Tagger[A]): DataFrame = {
    dataFrame.flatMap { row =>
      val index = row.fieldIndex(preparator.propertyName)
      val operatedValue = row.getAs[String](index)
      val (start, end) = row.toSeq.splitAt(index)
      val convertTry = convert(operatedValue, preparator.targetFormat).flatMap(phoneNumber => Try(Row.fromSeq(start ++ Seq(phoneNumber) ++ end)))

      convertTry match {
        case Failure(exception) => errorAccumulator.add(new RecordError(operatedValue, exception))
        case Success(_) => ()
      }

      convertTry.toOption

    }(RowEncoder(dataFrame.schema))
  }

  /**
    * Converting a phone number to a target format
    * @param phoneNumber String containing the phone number
    * @param targetFormat Source format of the phone number
    * @return Try of the converted phone number
    */
  def convert[A](phoneNumber: String, targetFormat: PhoneNumberFormat[A])(implicit tagger: Tagger[A]): Try[String] =
    PhoneNumber[A](phoneNumber).convert(targetFormat).map(_.toString)
}
