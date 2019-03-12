package de.hpi.isg.dataprep.preparators.implementation

import java.text.{DateFormat, ParseException, SimpleDateFormat}
import java.util.{Date, Locale}

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.AdaptiveChangeDateFormat
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object PartTypeEnum extends Enumeration {
  type PartTypeEnum = Value
  val  Number, Text, Mixed, Separator = Value
}

import de.hpi.isg.dataprep.preparators.implementation.PartTypeEnum._

case class PatternCriteria(numberOfBlocks: Int, separators: List[String], lengthOfNumberDateParts: List[Int], blockTypes: List[PartTypeEnum])

object AdaptiveDateUtils {
  val alphaNumericPattern: String = "[0-9a-zA-z\\x7f-\\xff]{1,}"
  val nonAlphaNumericPattern: String = "[^0-9a-zA-z\\x7f-\\xff]{1,}"
  val startsWithSeparatorPattern: String = "^[^0-9a-zA-z]{1,}"

  def getSimilarityCriteria(date: String): PatternCriteria = {

    // Regex parsing a date string with named capture groups datePart, separator
    val datePattern = new Regex(s"($alphaNumericPattern)|($nonAlphaNumericPattern)", "datePart", "separator")

    val dateBlocks: List[Regex.Match] = datePattern.findAllIn(date).matchData.toList
    val dateParts: List[String] = dateBlocks.filter(_.group("datePart") != null).map(_.toString).map(padSingleDigitDate)

    val numberOfBlocks: Int = dateBlocks.size
    val separators: List[String] = dateBlocks.filter(_.group("separator") != null).map(_.toString)
    val lengthOfNumberDateParts: List[Int] = dateParts.filter(isNumber).map(_.length)
    val blockTypes: List[PartTypeEnum] = dateBlocks.map(m => extractType(m.toString))
    PatternCriteria(numberOfBlocks, separators, lengthOfNumberDateParts, blockTypes)
  }

  def padSingleDigitDate(s: String): String = {
    if (s.length == 1 && s.forall(Character.isDigit))
      "0" + s
    else
      s
  }

  def extractType(s: String): PartTypeEnum = {
    if (AdaptiveDateUtils.isNumber(s)) {
      PartTypeEnum.Number
    } else if (AdaptiveDateUtils.isLetter(s)) {
      PartTypeEnum.Text
    } else if (AdaptiveDateUtils.isAlphaNumeric(s)) {
      PartTypeEnum.Mixed
    } else {
      PartTypeEnum.Separator
    }
  }

  def findValidLocalePattern(distinctBlockValues: Set[String]): Option[LocalePattern] = {
    val monthPattern = "MMM"
    val dayOfWeekPattern = "EEE"

    var localePattern: Option[LocalePattern] = None

    // First try US, then users locale and then all others
    val locales = Locale.US :: Locale.getDefault() :: DateFormat.getAvailableLocales.toList

    if (distinctBlockValues.size == 12) {
      localePattern = findValidLocale(distinctBlockValues, monthPattern, locales)
    } else if (distinctBlockValues.size == 7) {
      localePattern = findValidLocale(distinctBlockValues, dayOfWeekPattern, locales)
    } else {
      localePattern = findValidLocale(distinctBlockValues, monthPattern, locales)
      if (localePattern.isEmpty) {
        localePattern = findValidLocale(distinctBlockValues, dayOfWeekPattern, locales)
      }
    }
    localePattern
  }

  def findValidLocale(blockDomain: Set[String], pattern: String, locales: List[Locale]): Option[LocalePattern] = {
    locales.foreach(locale => {
      val sdf = new SimpleDateFormat(pattern, locale)
      if (!locale.getDisplayCountry.isEmpty) {
        blockDomain.foreach(value => {
          if (validDateFormat(value, sdf)) {
            println(s"Found valid Locale ${locale.toLanguageTag}")
            return Option(LocalePattern(locale, pattern))
          }
        })
      }
    })
    None
  }

  // Checks if value can be parsed
  def validDateFormat(value: String, sdf: SimpleDateFormat): Boolean = {
    Try{ sdf.parse(value) } match {
      case Failure(_) => false
      case Success(_) => true
    }
  }

  def isNumber(s: String): Boolean = {
    s.forall(_.isDigit)
  }

  def isLetter(s: String): Boolean = {
    s.forall(_.isLetter)
  }

  def isAlphaNumeric(s: String): Boolean = {
    s.forall(_.isLetterOrDigit)
  }

  def extractClusterDatePattern(dates: List[String]): Option[LocalePattern] = {
    println(s"Cluster: $dates")
    val simpleDates: List[SimpleDate] = dates.map(new SimpleDate(_))
    val blocks: List[List[String]] = simpleDates.map(_.splitDate).transpose
    val blockLocalePatterns: List[Option[LocalePattern]] = blocks
      .map(block => if (AdaptiveDateUtils.isLetter(block.head)) AdaptiveDateUtils.findValidLocalePattern(block.toSet) else None)

    var maybeLocaleDatePattern: Option[LocalePattern] = None

    simpleDates.iterator
      .takeWhile(_ => maybeLocaleDatePattern.isEmpty)
      .foreach(simpleDate =>
        maybeLocaleDatePattern = simpleDate.toPattern(blockLocalePatterns)
      )

    maybeLocaleDatePattern
  }

  def formatToTargetPattern(date: String, targetPattern: DatePatternEnum, maybeDatePattern: Option[LocalePattern])
  : String = {
    if (maybeDatePattern.isDefined) {
      val datePattern = maybeDatePattern.get
      val parsedDate = tryParseDate(date, datePattern.pattern, datePattern.locale)
      if (parsedDate.isDefined) {
        return getDateAsString(parsedDate.get, datePattern.pattern, datePattern.locale)
      }
    }
    throw new ParseException("No unambiguous pattern found to parse date. Date might be corrupted.", -1)
  }

  def getDateAsString(d: Date, pattern: String, locale: Locale): String = {
    val dateFormat = new SimpleDateFormat(pattern, locale)
    dateFormat.setLenient(false)
    dateFormat.format(d)
  }

  def tryParseDate(dateString: String, datePattern: String, locale: Locale): Option[Date] = {
    Try{ convertStringToDate(dateString, datePattern, locale) } match {
      case Failure(_) => None
      case Success(date) => Some(date)
    }
  }

  def convertStringToDate(s: String, pattern: String, locale: Locale): Date = {
    val dateFormat = new SimpleDateFormat(pattern, locale)
    dateFormat.setLenient(false)
    dateFormat.parse(s)
  }
}

/**
  * Converts source to target date pattern
  * If no source pattern is given,
  * all possible unambiguous date patterns are extracted and applied in order of occurrence
  * @author Hendrik Rätz, Nils Strelow
  * @since 2018/12/03
  */
class DefaultAdaptiveChangeDateFormatImpl extends AbstractPreparatorImpl with Serializable {

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row],
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[AdaptiveChangeDateFormat]
    val propertyName = preparator.propertyName
    val sourceDatePattern = preparator.sourceDatePattern
    val targetDatePattern = preparator.targetDatePattern

    val rowEncoder = RowEncoder(dataFrame.schema)

    val dateClusterPatterns = dataFrame.rdd
      .map(_.getAs[String](propertyName))
      .groupBy(AdaptiveDateUtils.getSimilarityCriteria)
      .mapValues(clusteredDates => AdaptiveDateUtils.extractClusterDatePattern(clusteredDates.toList))
      .collect()
      .toMap[PatternCriteria, Option[LocalePattern]]

    val createdDataset = dataFrame.flatMap(row => {
      val operatedValue = row.getAs[String](propertyName)

      val indexTry = Try{row.fieldIndex(propertyName)}
      val index = indexTry match {
        case Failure(content) => throw content
        case Success(content) => content
      }

      val seq = row.toSeq
      val forepart = seq.take(index)
      val backpart = seq.takeRight(row.length-index-1)

      val tryConvert = Try{
        if (sourceDatePattern.isDefined) {
          val newSeq = forepart :+ ConversionHelper.toDate(operatedValue, sourceDatePattern.get, targetDatePattern)
          val newRow = Row.fromSeq(newSeq)
          newRow
        } else {
          val localePatternOption: Option[LocalePattern] = dateClusterPatterns(AdaptiveDateUtils.getSimilarityCriteria(operatedValue))
          val newSeq = forepart :+ AdaptiveDateUtils.formatToTargetPattern(operatedValue, targetDatePattern,
            localePatternOption)
          val newRow = Row.fromSeq(newSeq)
          newRow
        }
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
    createdDataset.count()

    new ExecutionContext(createdDataset, errorAccumulator)
  }

}
