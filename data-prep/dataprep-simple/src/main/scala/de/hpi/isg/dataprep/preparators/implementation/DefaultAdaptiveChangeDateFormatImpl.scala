package de.hpi.isg.dataprep.preparators.implementation

import java.text.{ParseException, SimpleDateFormat}
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

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

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

    val extractedPatterns = dataFrame.rdd
      .map(_.getAs[String](propertyName))
      .map(toPattern)
      .filter(_.isDefined)
      .map(_.get)
      .map((_, 1L))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()
      .toMap

    println(extractedPatterns)

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
          val newSeq = forepart :+ formatToTargetPattern(operatedValue, targetDatePattern, extractedPatterns)
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

  def getDateAsString(d: Date, pattern: String): String = {
    // TODO(ns): Set locale to US, need to find a way to parse multiple languages
    val dateFormat = new SimpleDateFormat(pattern, Locale.US)
    dateFormat.setLenient(false)
    dateFormat.format(d)
  }

  def convertStringToDate(s: String, pattern: String): Date = {
    // TODO(ns): Set locale to US, need to find a way to parse multiple languages
    val dateFormat = new SimpleDateFormat(pattern, Locale.US)
    dateFormat.setLenient(false)
    dateFormat.parse(s)
  }

  def formatToTargetPattern(date: String, targetPattern: DatePatternEnum, extractedPatterns: Map[String, Long]): String = {
    for((pattern, count) <- extractedPatterns) {
      breakable {
        println(s"DateString: $date")
        println(s"Try pattern: $pattern")
        val parsedDateTry = Try {
          convertStringToDate(date, pattern)
        }
        val parsedDate = parsedDateTry match {
          case Success(content) => content
          case Failure(content) => break;
        }

        println(s"Pattern succeeded: $parsedDate")
        return getDateAsString(parsedDate, targetPattern.getPattern)
      }
    }
    throw new ParseException("No unambiguous pattern found to parse date. Date might be corrupted.", -1)
  }

  def padSingleDigitDates(dates: List[String]): List[String] = {
    dates.map( date => if (date.length == 1 && date.forall(Character.isDigit)) "0" + date else date )
  }

  private def convertMonthNames(splittedDate: List[String]): (List[String], Option[String]) = {
    val monthNameToNumber = Map(
      "January"   -> 1,
      "February"  -> 2,
      "March"     -> 3,
      "April"     -> 4,
      "May"       -> 5,
      "June"      -> 6,
      "July"      -> 7,
      "August"    -> 8,
      "September" -> 9,
      "October"   -> 10,
      "November"  -> 11,
      "December"  -> 12
    )
    var convertedMonth: String = ""
    var newDate: List[String] = splittedDate

    for ((block, index) <- splittedDate.view.zipWithIndex) {
      if (block.forall(_.isLetter)) {
        for (month <- monthNameToNumber.keys) {
          if (month.toLowerCase().startsWith(block.toLowerCase())) {
            // TODO: Early abort may lead to errors if multiple strings are present, e.g. DoW and month
            convertedMonth = f"${monthNameToNumber(month)}%02d"
            newDate = newDate.updated(index, convertedMonth)
            return (newDate, Some(convertedMonth))
          }
        }
      }
    }

    (newDate, None)
  }

  def generatePlaceholder(origString:String, placeholder:String):String = {
    origString.replaceAll(".", placeholder)
  }

  def generatePattern(fullGroup:List[String], separators: List[String], startsWithSeperator: Boolean, year: String, month: String, day: String): String = {
    //TODO: Fix generation for now separators are missing
    println(s"generatePattern: $fullGroup, $separators, $year, $month, $day")
    var newGroup = fullGroup.updated(fullGroup.indexOf(year), generatePlaceholder(year, "y"))
    newGroup = newGroup.updated(newGroup.indexOf(month), generatePlaceholder(month, "M"))
    newGroup = newGroup.updated(newGroup.indexOf(day), generatePlaceholder(day, "d"))

    val separatorsBuffer: ListBuffer[String] = separators.to[ListBuffer]

    var pattern: String = ""

    if (startsWithSeperator) {
      // Pop first element
      pattern = separatorsBuffer.remove(0)
      println(separators)
    }

    for(group <- newGroup) {
      var groupAsString = group.toString

      // Full text pattern for month is MMMM
      if (groupAsString.startsWith("MMMM")) {
        groupAsString = "MMMM"
      }

      val seperator = if(separatorsBuffer.nonEmpty) separatorsBuffer.remove(0) else ""

      pattern = pattern + groupAsString + seperator
    }

    pattern
  }

  def toPattern(date: String): Option[String] = {
    var yearOption: Option[String] = None
    var monthOption: Option[String] = None
    var dayOption: Option[String] = None
    println(s"Date: $date")

    val splitDate: List[String] = date.split("[^0-9a-zA-z]{1,}").toList
    println(s"Splits: $splitDate")
    if (splitDate.length > 3) {
      return None // Assumption that date only contains day, month and year
    }

    val separatorPattern: Regex = "[^0-9a-zA-z]{1,}".r
    val startsWithSeparatorPattern: String = "^[^0-9a-zA-z]{1,}"
    val startsWithSeparator: Boolean = date.matches(startsWithSeparatorPattern)
    val separators: List[String] = separatorPattern.findAllMatchIn(date).map(_.toString).toList
    println("Separators: " + separators + " starts with separator: " + startsWithSeparator)

    val (leftoverBlocks, convertedMonth) = convertMonthNames(splitDate)
    var undeterminedBlocks: List[String] = padSingleDigitDates(leftoverBlocks)

    // Used for patternGeneration later
    val monthText: Option[String] =  splitDate.find(!leftoverBlocks.contains(_))

    monthOption = convertedMonth
    println(s"-> Month: $monthOption")

    if (monthOption.isDefined) {
      undeterminedBlocks = undeterminedBlocks.filter(_ != monthOption.get)
    }

    def applyRules(_undeterminedBlocks: List[String]): Unit = {
      // TODO: If multiple blocks are the same, it wouldn't matter which is which
      var leftoverBlocks: List[String] = _undeterminedBlocks

      for (block <- _undeterminedBlocks) { // if no there are no undetermined parts left, function will return
        breakable {
          if (block.toInt <= 0) {
            break
          }
          if (yearOption.isEmpty && (block.length == 4 || block.toInt > 31 || (dayOption.isDefined && block.toInt > 12))) {
            yearOption = Some(block)
          } else if (dayOption.isEmpty && (block.toInt > 12 && block.toInt <= 31 ||
            (monthOption.isDefined && block.toInt > 0 && block.toInt <= 31))) {
            dayOption = Some(block)
          } else {
            break
          }
          leftoverBlocks = leftoverBlocks.filter(_ != block)
        }
      }

      // assign leftover block if there is any
      if (leftoverBlocks.length == 1) {
        println("Check")
        if (yearOption.isEmpty) {
          yearOption = Some(leftoverBlocks.head)
        } else if (monthOption.isEmpty) {
          monthOption = Some(leftoverBlocks.head)
        } else if (dayOption.isEmpty) {
          dayOption = Some(leftoverBlocks.head)
        }
      }
    }

    applyRules(undeterminedBlocks)

    println(s"$yearOption, $monthOption, $dayOption")
    if (yearOption.isDefined && monthOption.isDefined && dayOption.isDefined) {

      // Use textual month, if provided, to generate correct pattern
      val month = monthText.getOrElse(monthOption.get)

      val resultingPattern = generatePattern(splitDate, separators, startsWithSeparator, yearOption.get, month, dayOption.get)
      println(s"Result: $resultingPattern\n")
      return Some(resultingPattern)
    }
    println("")
    None
  }

}
