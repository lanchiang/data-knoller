package de.hpi.isg.dataprep.preparators.implementation

import java.text.{DateFormat, ParseException, SimpleDateFormat}
import java.util.Locale

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.AdaptiveChangeDateFormat
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import de.hpi.isg.dataprep.{ConversionHelper, DatasetUtils, ExecutionContext, StringUtils}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.CollectionAccumulator

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object DatePartTypeEnum extends Enumeration {
  type DatePartTypeEnum = Value
  val  Number, Text, Mixed, Separator = Value
}

import de.hpi.isg.dataprep.preparators.implementation.DatePartTypeEnum._

case class PatternCriteria(numberOfBlocks: Int, separators: List[String], lengthOfNumberDateParts: List[Int], blockTypes: List[DatePartTypeEnum])

/**
  * Converts source to target date pattern.
  * If no source pattern is given, all possible unambiguous date patterns are extracted and applied in order of occurrence
  *
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

    val intermediate = dataFrame.withColumn(propertyName + "_reformatted", lit(""))
    val rowEncoder = RowEncoder(intermediate.schema)

    val dateClusterPatterns = dataFrame.rdd
            .map(_.getAs[String](propertyName))
            .groupBy(ChangeDateFormatUtils.getSimilarityCriteria) // may be null
            .mapValues(clusteredDates => ChangeDateFormatUtils.extractClusterDatePattern(clusteredDates.toList))
            .collect()
            .toMap[PatternCriteria, Option[LocalePattern]]

    val createdDataset = intermediate.flatMap(row => {
      val index = DatasetUtils.getFieldIndexByPropertyNameSafe(row, propertyName)
      val operatedValue = row.getAs[String](propertyName)

      val seq = row.toSeq
      val forepart = seq.take(index)
      val backpart = seq.takeRight(row.length-index-1)

      // Todo: now the formatted values overwrite the original column
      val tryConvert = Try{
        if (sourceDatePattern.isDefined) {
          val newSeq = (forepart :+ ConversionHelper.toDate(operatedValue, sourceDatePattern.get, targetDatePattern)) ++ backpart
          val newRow = Row.fromSeq(newSeq)
          newRow
        } else {
          val localePatternOption: Option[LocalePattern] = dateClusterPatterns(ChangeDateFormatUtils.getSimilarityCriteria(operatedValue))
          val newSeq = (forepart :+ ChangeDateFormatUtils.formatToTargetPattern(operatedValue, targetDatePattern, localePatternOption)) ++ backpart
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

object ChangeDateFormatUtils {
  val alphaNumericPattern: String = "[0-9a-zA-z\\x7f-\\xff]{1,}"
  val nonAlphaNumericPattern: String = "[^0-9a-zA-z\\x7f-\\xff]{1,}"
  val startsWithSeparatorPattern: String = "^[^0-9a-zA-z]{1,}"

  def getSimilarityCriteria(date: String): PatternCriteria = {
    // Regex parsing a date string with named capture groups datePart, separator
    val datePattern = new Regex(s"($alphaNumericPattern)|($nonAlphaNumericPattern)", "datePart", "separator")

    // Todo: deal with null
    val patternCriteria = Try{
      datePattern.findAllIn(date).matchData.toList
    } match {
      case Success(value) => {
        val dateBlocks = value
        val dateParts = dateBlocks.filter(_.group("datePart") != null).map(_.toString).map(StringUtils.padSingleZeroToDateComponent)

        val numberOfBlocks = dateBlocks.size
        val separators = dateBlocks.filter(_.group("separator") != null).map(_.toString)
        val lengthOfNumberDateParts = dateParts.filter(StringUtils.isNumber).map(_.length)
        val blockTypes = dateBlocks.map(m => findDatePartType(m.toString))
        PatternCriteria(numberOfBlocks, separators, lengthOfNumberDateParts, blockTypes)
      }
      case Failure(exception) => null
    }
    patternCriteria
//    val dateParts = dateBlocks.filter(_.group("datePart") != null).map(_.toString).map(StringUtils.padSingleZeroToDateComponent)
//
//    val numberOfBlocks = dateBlocks.size
//    val separators = dateBlocks.filter(_.group("separator") != null).map(_.toString)
//    val lengthOfNumberDateParts = dateParts.filter(StringUtils.isNumber).map(_.length)
//    val blockTypes = dateBlocks.map(m => findDatePartType(m.toString))
//    PatternCriteria(numberOfBlocks, separators, lengthOfNumberDateParts, blockTypes)
  }

  /**
    * Finds the component type of the given part of a date string. The type can be one of the following: Number, Text, Mixed, Separator.
    *
    * @param s part of a date string.
    * @return the type of the given date part
    */
  def findDatePartType(s: String): DatePartTypeEnum = {
    if (StringUtils.isNumber(s)) {
      DatePartTypeEnum.Number
    } else if (StringUtils.isLetter(s)) {
      DatePartTypeEnum.Text
    } else if (StringUtils.isAlphaNumeric(s)) {
      DatePartTypeEnum.Mixed
    } else {
      DatePartTypeEnum.Separator
    }
  }

  def findValidLocalePattern(distinctBlockValues: Set[String]): Option[LocalePattern] = {
    val monthPattern = "MMM"
    val dayOfWeekPattern = "EEE"

    // First try US, then users locale and then all others
    val locales = Locale.US :: Locale.getDefault() :: DateFormat.getAvailableLocales.toList

    var localePattern: Option[LocalePattern] = None
    localePattern = distinctBlockValues.size match {
      case 12 => findValidLocale(distinctBlockValues, monthPattern, locales)
      case 7 => findValidLocale(distinctBlockValues, dayOfWeekPattern, locales)
      case _ => {
        localePattern = findValidLocale(distinctBlockValues, monthPattern, locales)
        localePattern.isEmpty match {
          case true => findValidLocale(distinctBlockValues, dayOfWeekPattern, locales)
          case false => localePattern
        }
      }
    }
    localePattern
  }

  /**
    * Returns the locale pattern out of the given locale set that can parse this date string
    *
    * @param blockDomain is the block of values of which the locale needs to be found
    * @param pattern the used pattern (MMM or EEE)
    * @param locales is the given locale set
    * @return the locale pattern that can parse the date string values in this block
    */
  def findValidLocale(blockDomain: Set[String], pattern: String, locales: List[Locale]): Option[LocalePattern] = {
    locales.foreach(locale => {
      val sdf = new SimpleDateFormat(pattern, locale)
      if (!locale.getDisplayCountry.isEmpty) {
        blockDomain.foreach(value => {
          if (StringUtils.isParsableDateFormat(value, sdf)) {
            println(s"Found valid Locale ${locale.toLanguageTag}")
            return Option(LocalePattern(locale, pattern))
          }
        })
      }
    })
    None
  }

  def extractClusterDatePattern(dates: List[String]): Option[LocalePattern] = {
    println(s"Cluster: $dates")
    val simpleDates: List[SimpleDate] = dates.map(new SimpleDate(_))
    val blocks: List[List[String]] = simpleDates.map(_.splitDate).transpose
    val blockLocalePatterns: List[Option[LocalePattern]] = blocks
      .map(block => if (StringUtils.isLetter(block.head)) ChangeDateFormatUtils.findValidLocalePattern(block.toSet) else None)

    var maybeLocaleDatePattern: Option[LocalePattern] = None

    simpleDates.iterator
      .takeWhile(_ => maybeLocaleDatePattern.isEmpty)
      .foreach(simpleDate =>
        maybeLocaleDatePattern = simpleDate.toPattern(blockLocalePatterns)
      )
    maybeLocaleDatePattern
  }

  /**
    * Returns the date string in the format and locale given by the corresponding parameters.
    *
    * @param date the date string whose format is to be changed
    * @param targetPattern the target date pattern
    * @param maybeDatePattern the current date pattern
    * @return the reformatted date string
    */
  def formatToTargetPattern(date: String, targetPattern: DatePatternEnum, maybeDatePattern: Option[LocalePattern]): String = {
    if (maybeDatePattern.isDefined) {
      val datePattern = maybeDatePattern.get
      val parsedDate = StringUtils.tryParseDate(date, datePattern.pattern, datePattern.locale)
      if (parsedDate.isDefined) {
        return StringUtils.getDateAsString(parsedDate.get, targetPattern.getPattern, datePattern.locale)
      }
    }
    throw new ParseException("No unambiguous pattern found to parse date. Date might be corrupted.", -1)
  }
}
