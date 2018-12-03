package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.metadata.DINPhoneNumber
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangePhoneFormat
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class DefaultChangePhoneFormatImpl extends PreparatorImpl with Serializable {
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
    val sourceFormat = Option(preparator.sourceFormat)
    val targetFormat = preparator.targetFormat

    val rowEncoder = RowEncoder(dataFrame.schema)

    val createdDataset = dataFrame.flatMap(row => {
      val indexTry = Try { row.fieldIndex(propertyName) }
      val index = indexTry match {
        case Failure(content) => throw content
        case Success(content) => content
      }

      val operatedValue = row.getAs[String](index)

      val seq = row.toSeq
      val forepart = seq.take(index)
      val backpart = seq.takeRight(row.length-index-1)

      val convertOption = sourceFormat
        .fold(convert(operatedValue, targetFormat))(source => convert(operatedValue, source, targetFormat))
        .map(number => Row.fromSeq(forepart ++ Seq(number) ++ backpart))

      if (convertOption.isEmpty) errorAccumulator.add(new RecordError(operatedValue, new IllegalArgumentException))

      convertOption

    })(rowEncoder)

    createdDataset.persist()

    createdDataset.count()

    new ExecutionContext(createdDataset, errorAccumulator)
  }

  final case class NormalizedPhoneNumber(
    number: String,
    optCountryCode: Option[String] = None,
    optAreaCode: Option[String] = None,
    optSpecialNumber: Option[String] = None,
    optExtensionNumber: Option[String] = None
  ) extends {
    override def toString: String = {
      val prefix = List(optCountryCode, optAreaCode, optSpecialNumber).flatten.mkString(" ")
      val postFix = optExtensionNumber.fold("")(extension => s"-$extension")

      s"$prefix $number$postFix"
    }
  }

  object NormalizedPhoneNumber {
    def fromMeta(meta: DINPhoneNumber)(phoneNumber: String): Option[NormalizedPhoneNumber] = {
      meta.getRegex.findFirstMatchIn(phoneNumber).map { matched =>

        val format = Map("countryCode" -> meta.getCountryCode, "areaCode" -> meta.getAreaCode, "specialNumber" -> meta.getSpecialNumber, "extensionNumber" -> meta.getExtensionNumber)
        val number = matched.group("number")

        format.filter(_._2).keySet.foldLeft(NormalizedPhoneNumber(number)) {
          case (normalized, "countryCode") => normalized.copy(optCountryCode = Some(matched.group("countryCode")))
          case (normalized, "areaCode") => normalized.copy(optAreaCode = Some(matched.group("areaCode")))
          case (normalized, "specialNumber") => normalized.copy(optSpecialNumber = Some(matched.group("specialNumber")))
          case (normalized, "extensionNumber") => normalized.copy(optExtensionNumber = Some(matched.group("extensionNumber")))
          case (normalized, _) => normalized
        }
      }
    }

    def toMeta(meta: DINPhoneNumber)(normalized: NormalizedPhoneNumber): String = {
      val format = Map("countryCode" -> meta.getCountryCode, "areaCode" -> meta.getAreaCode, "specialNumber" -> meta.getSpecialNumber, "extensionNumber" -> meta.getExtensionNumber)

      format.filterNot(_._2).keySet.foldLeft(normalized) {
        case (phoneNumber, "countryCode") => phoneNumber.copy(optCountryCode = None)
        case (phoneNumber, "areaCode") => phoneNumber.copy(optAreaCode = None)
        case (phoneNumber, "specialNumber") => phoneNumber.copy(optSpecialNumber = None)
        case (phoneNumber, "extensionNumber") => phoneNumber.copy(optExtensionNumber = None)
        case (phoneNumber, _) => phoneNumber
      }.toString
    }
  }

  /*
  private def convertPhoneNumber(phoneNumber: String, sourceFormat: String, targetFormat: String) : String = {
    val digitsOnly = phoneNumber.replaceFirst("\\+", "00").replaceAll("\\D", "")
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
  */

  private def convert(phoneNumber: String, sourceFormat: DINPhoneNumber, targetFormat: DINPhoneNumber): Option[String] = {
    NormalizedPhoneNumber.fromMeta(sourceFormat)(phoneNumber).map(NormalizedPhoneNumber.toMeta(targetFormat))
  }

  private def convert(phoneNumber: String, targetFormat: DINPhoneNumber): Option[String] = {
    val areaCoded = new Regex("""(\d+) (\d+)""", "areaCode", "number")
    val extended = new Regex("""(\d+) (\d+)-(\d+)""", "areaCode", "number", "extension")
    val specialNumbered = new Regex("""(\d+) (\d) (\d+)""", "areaCode", "specialNumber", "number")
    val countryCoded = new Regex("""(\+\d{2}) (\d+) (\d+)""", "countryCode", "areaCode", "number")

    val sourceFormat = phoneNumber match {
      case areaCoded(_*) => new DINPhoneNumber(false, true, false, false, areaCoded)
      case extended(_*) => new DINPhoneNumber(false, true, false, true, extended)
      case specialNumbered(_*) => new DINPhoneNumber(false, true, true, false, specialNumbered)
      case countryCoded(_*) => new DINPhoneNumber(true, true, false, false, countryCoded)
    }

    convert(phoneNumber, sourceFormat, targetFormat)
  }
}
