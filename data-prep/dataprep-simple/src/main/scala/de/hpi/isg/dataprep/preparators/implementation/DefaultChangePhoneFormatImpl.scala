package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangePhoneFormat
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.{Failure, Success, Try}

class DefaultChangePhoneFormatImpl extends PreparatorImpl {
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
    val propertyName = preparator.propertyName
    val sourceFormat = preparator.sourceFormat
    val targetFormat = preparator.targetFormat

    val rowEncoder = RowEncoder(dataFrame.schema)

    val createdDataset = dataFrame.flatMap(row => {
      val indexTry = Try {
        row.fieldIndex(propertyName)
      }
      val index = indexTry match {
        case Failure(content) => {
          throw content
        }
        case Success(content) => {
          content
        }
      }
      val operatedValue = row.getAs[String](index)

      val tryConvert = Try {
        val newSeq = convertPhoneNumber(operatedValue, sourceFormat, targetFormat)
        Row.fromSeq(newSeq)
      }
      val convertOption = tryConvert match {
        case Failure(content) => {
          errorAccumulator.add(new RecordError(operatedValue, content))
          tryConvert
        }
        case Success(content) => tryConvert
      }
      convertOption.toOption
    })(rowEncoder)

    createdDataset.persist()

    createdDataset.count()

    new ExecutionContext(createdDataset, errorAccumulator)
  }

  private def convertPhoneNumber(phoneNumber: String, sourceFormat: String, targetFormat: String) : String = {
    val digitsOnly = phoneNumber.replaceFirst("+", "00").replaceAll(raw"""\D""", "")
    var formattedPhoneNumber = ""
    var digitsOnlyCount = 0

    //TODO: target[0] == +, but digitsOnly[0,1] != 00

    //Insert non-digit characters from targetFormat into stripped phoneNumber
    for (i <- 0 until targetFormat.length()) {
      val targetChar = targetFormat.charAt(i)
      if (targetChar.eq('d')) {
        formattedPhoneNumber += digitsOnly.charAt(digitsOnlyCount)
        digitsOnlyCount += 1
      } else {
        formattedPhoneNumber += targetChar
      }
    }

    //append remaining digits from phoneNumber if longer than targetFormat
    for (i <- digitsOnlyCount until digitsOnly.length) {
      formattedPhoneNumber += digitsOnly.charAt(i)
    }
    formattedPhoneNumber
  }
}

