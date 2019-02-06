package de.hpi.isg.dataprep.preparators.implementation

import java.text.{DateFormat, ParseException, SimpleDateFormat}
import java.util.{Date, Locale}

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.AdaptiveChangeDateFormat
import de.hpi.isg.dataprep.preparators.implementation.PartTypeEnum.PartTypeEnum
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object Utils {
  def getSimilarityCriteria(date: String): PatternCriteria = {
    val alphaNumericPattern = "[0-9a-zA-z\\x7f-\\xff]{1,}"
    val nonAlphaNumericPattern = "[^0-9a-zA-z\\x7f-\\xff]{1,}"
    // Regex parsing a date string with named capture groups datePart, separator
    val datePattern = new Regex(s"($alphaNumericPattern)|($nonAlphaNumericPattern)", "datePart", "separator")

    val initialDateBlocks: List[Regex.Match] = datePattern.findAllIn(date).matchData.toList
    val dateParts: List[String] = initialDateBlocks.filter(_.group("datePart") != null).map(_.toString)

    val numberOfBlocks: Int = initialDateBlocks.size
    val separators: List[String] = initialDateBlocks.filter(_.group("separator") != null).map(_.toString)
    // TODO(ns) Pad before calculating this, also months with different lengths
    val lengthOfDateParts: List[Int] = dateParts.map(_.length)
    val blockTypes: List[PartTypeEnum] = initialDateBlocks.map(m => extractType(m.toString))
    PatternCriteria(numberOfBlocks, separators, lengthOfDateParts, blockTypes)
  }

  def extractType(s: String): PartTypeEnum = {
    if (Utils.isDigit(s)) {
      PartTypeEnum.Number
    } else if (Utils.isLetter(s)) {
      PartTypeEnum.Text
    } else if (Utils.isAlphaNumeric(s)) {
      PartTypeEnum.Mixed
    } else {
      PartTypeEnum.Separator
    }
  }

  def findValidLocalePattern(distinctBlockValues: Set[String]): Option[LocalePattern] = {
    val monthPattern = "MMM"
    val dayOfWeekPattern = "EEE"

    var localePattern: Option[LocalePattern] = None

    if (distinctBlockValues.size == 12) {
      localePattern = findValidLocale(distinctBlockValues, monthPattern, DateFormat.getAvailableLocales.toList)
    } else if (distinctBlockValues.size == 7) {
      localePattern = findValidLocale(distinctBlockValues, dayOfWeekPattern, DateFormat.getAvailableLocales.toList)
    } else {
      localePattern = findValidLocale(distinctBlockValues, monthPattern, DateFormat.getAvailableLocales.toList)
      if (localePattern.isEmpty) {
        localePattern = findValidLocale(distinctBlockValues, dayOfWeekPattern, DateFormat.getAvailableLocales.toList)
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

  def isDigit(s: String): Boolean = {
    s.forall(_.isDigit)
  }

  def isLetter(s: String): Boolean = {
    s.forall(_.isLetter)
  }

  def isAlphaNumeric(s: String): Boolean = {
    s.forall(_.isLetterOrDigit)
  }

  def extractClusterDatePattern(dates: List[String]): Option[String] = {
    val simpleDates: List[SimpleDate] = dates.map(new SimpleDate(_))
    val blocks: List[List[String]] = simpleDates.map(_.splitDate).transpose
    val blockLocalePatterns: List[Option[LocalePattern]] = blocks
      // return None, when it's not a letter
      .map(block => if (Utils.isLetter(block.head)) Utils.findValidLocalePattern(block.toSet) else None)

    var maybeDatePattern: Option[String] = None

    simpleDates.iterator
      .takeWhile(_ => maybeDatePattern.isEmpty)
      .foreach(simpleDate => maybeDatePattern = simpleDate.toPattern(blockLocalePatterns))

    maybeDatePattern
  }
}

object PartTypeEnum extends Enumeration {
  type PartTypeEnum = Value
  val  Number, Text, Mixed, Separator = Value
}

case class PatternCriteria(numberOfBlocks: Int, separators: List[String], lengthOfDateParts: List[Int], blockTypes: List[PartTypeEnum])

/**
  * Converts source to target date pattern
  * If no source pattern is given,
  * all possible unambiguous date patterns are extracted and applied in order of occurrence
  * @author Hendrik Rätz, Nils Strelow
  * @since 2018/12/03
  */
class DefaultAdaptiveChangeDateFormatImpl extends AbstractPreparatorImpl with Serializable {

  override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[AdaptiveChangeDateFormat]
    val propertyName = preparator.propertyName
    val sourceDatePattern = preparator.sourceDatePattern
    val targetDatePattern = preparator.targetDatePattern

    val rowEncoder = RowEncoder(dataFrame.schema)

    val dateClusterPatterns = dataFrame.rdd
      .map(_.getAs[String](propertyName))
      .groupBy(Utils.getSimilarityCriteria)
      .mapValues(clusteredDates => Utils.extractClusterDatePattern(clusteredDates.toList))
      .collect()
      .toMap[PatternCriteria, Option[String]]

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
          val newSeq = forepart :+ formatToTargetPattern(operatedValue, targetDatePattern, dateClusterPatterns)
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

  def formatToTargetPattern(date: String, targetPattern: DatePatternEnum, dateClustersPatterns: Map[PatternCriteria, Option[String]]): String = {
    val similarityCriteria: PatternCriteria = Utils.getSimilarityCriteria(date)
    val maybeDatePattern: Option[String] = dateClustersPatterns(similarityCriteria)

    if (maybeDatePattern.isDefined) {
      val parsedDate = tryParseDate(date, maybeDatePattern.get)
      if (parsedDate.isDefined) {
        return getDateAsString(parsedDate.get, targetPattern.getPattern)
      }
    }
    throw new ParseException("No unambiguous pattern found to parse date. Date might be corrupted.", -1)
  }

  def getDateAsString(d: Date, pattern: String): String = {
    // TODO(ns): Set locale to US, need to find a way to parse multiple languages
    val dateFormat = new SimpleDateFormat(pattern, Locale.US)
    dateFormat.setLenient(false)
    dateFormat.format(d)
  }

  def tryParseDate(dateString: String, datePattern: String): Option[Date] = {
    Try{ convertStringToDate(dateString, datePattern) } match {
      case Failure(_) => None
      case Success(date) => Some(date)
    }
  }

  def convertStringToDate(s: String, pattern: String): Date = {
    // TODO(ns): Set locale to US, need to find a way to parse multiple languages
    val dateFormat = new SimpleDateFormat(pattern, Locale.US)
    dateFormat.setLenient(false)
    dateFormat.parse(s)
  }
}
