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

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


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

  def applyRules(undeterminedBlocks: List[String], date: SimpleDate): SimpleDate = {
    // TODO: If multiple blocks are the same, it wouldn't matter which is which
    var leftoverBlocks: List[String] = undeterminedBlocks
    val updatedDate: SimpleDate = date

    for (block <- undeterminedBlocks) { // if no there are no undetermined parts left, function will return
      breakable {
        if (block.toInt <= 0) {
          break
        }
        if (!date.isYearDefined && (block.length == 4 || block.toInt > 31 || (date.isDayDefined && block.toInt > 12))) {
          updatedDate.yearOption = Some(block)
        } else if (!date.isDayDefined && (block.toInt > 12 && block.toInt <= 31 ||
          (date.isMonthDefined && block.toInt > 0 && block.toInt <= 31))) {
          updatedDate.dayOption = Some(block)
        } else {
          break
        }
        leftoverBlocks = leftoverBlocks.filter(_ != block)
      }
    }

    // assign leftover block if there is any
    if (leftoverBlocks.length == 1) {
      println("Check")
      if (!date.isYearDefined) {
        updatedDate.yearOption = Some(leftoverBlocks.head)
      } else if (!date.isMonthDefined) {
        updatedDate.monthOption = Some(leftoverBlocks.head)
      } else if (!date.isDayDefined) {
        updatedDate.dayOption = Some(leftoverBlocks.head)
      }
    }
    updatedDate
  }

  def generatePlaceholder(origString:String, placeholder:String):String = {
    origString.replaceAll(".", placeholder)
  }

  def generatePattern(date: SimpleDate): String = {
    var pattern: String = ""
    val year = date.yearOption.get
    val month = date.monthText.getOrElse(date.monthOption.get)
    val day = date.dayOption.get
    println(s"generatePattern: $date.fullGroup, ${date.getSeparators}, $year, $month, $day")

    var newGroup = date.splitDate.updated(date.splitDate.indexOf(year), generatePlaceholder(year, "y"))
    newGroup = newGroup.updated(newGroup.indexOf(month), generatePlaceholder(month, "M"))
    newGroup = newGroup.updated(newGroup.indexOf(day), generatePlaceholder(day, "d"))

    val separatorsBuffer: ListBuffer[String] = date.getSeparators.to[ListBuffer]

    if (date.startsWithSeparator) {
      // Pop first element
      pattern = separatorsBuffer.remove(0)
      println(date.getSeparators)
    }

    for(group <- newGroup) {
      var groupAsString = group.toString

      // Full text pattern for month is MMMM
      if (groupAsString.startsWith("MMMM")) {
        groupAsString = "MMMM"
      }

      val separator = if(separatorsBuffer.nonEmpty) separatorsBuffer.remove(0) else ""

      pattern = pattern + groupAsString + separator
    }

    pattern
  }

  def toPattern(originalDate: String): Option[String] = {
    var date = new SimpleDate(originalDate)
    println(s"Date: $originalDate")

    date = applyRules(date.undeterminedBlocks, date)

    println(s"$date")
    if (date.isDefined) {
      val resultingPattern = generatePattern(date)
      println(s"Result: $resultingPattern\n")
      return Some(resultingPattern)
    }
    println("")
    None
  }

  case class TextDateField(pattern: String, locale: Locale)

  def findValidPatternAndLocale(cluster: List[List[String]]): Map[Int, Option[TextDateField]] = {
    val monthPattern = "MMM"
    val dayOfWeekPattern = "E"

    println(DateFormat.getAvailableLocales.toList)

    val blocks = cluster.transpose
    val blockTestDateFields = blocks.zipWithIndex
      .filter{case (block, _) => block.head.forall(_.isLetter)}
      .map{case (block, index) => {

        val blockDomain = block.toSet
        var textDateField: Option[TextDateField] = None
        if (blockDomain.size == 12) {
          textDateField = findValidLocale(blockDomain, monthPattern, DateFormat.getAvailableLocales.toList)
        } else if (blockDomain.size == 7) {
          textDateField = findValidLocale(blockDomain, dayOfWeekPattern, DateFormat.getAvailableLocales.toList)
        } else {
          textDateField = findValidLocale(blockDomain, monthPattern, DateFormat.getAvailableLocales.toList)
          if (textDateField.isEmpty) {
            textDateField = findValidLocale(blockDomain, dayOfWeekPattern, DateFormat.getAvailableLocales.toList)
          }
        }
        (index, textDateField)
      }}.toMap
    blockTestDateFields
  }

  def findValidLocale(blockDomain: Set[String], pattern: String, locales: List[Locale]): Option[TextDateField] = {
    locales.foreach(locale => {
      val sdf = new SimpleDateFormat(pattern, locale)
      if (!locale.getDisplayCountry.isEmpty) {
        blockDomain.foreach(value => {
          if (validDateFormat(value, sdf)) {
            println(locale.toLanguageTag)
            return Option(TextDateField(pattern, locale))
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


  // Clustering Start
  def isDigit(s: String): Boolean = {
    s.forall(_.isDigit)
  }

  def isLetter(s: String): Boolean = {
    s.forall(_.isLetter)
  }

  def isAlphanumeric(s: String): Boolean = {
    s.forall(_.isLetterOrDigit)
  }

  def splitString(s: String): List[String] = {
    // Currently separators are split too so that ", " would become two different blocks: [,] [ ]
    // This is fine for now because our clustering is very strict
    val tmp: String = (s + 'X')
      .sliding(2)
      .map(pair => pair.head +
        (if (isAlphanumeric(pair.head.toString) && isAlphanumeric(pair.last.toString))
          ""
        else
          "&"))
      .mkString("")
    tmp.split("&").toList
  }

  def getSimilarity(s1: String, s2: String): Double  = {
    val split1: List[String] = splitString(s1)
    val split2: List[String] = splitString(s2)
    var score: Double = 0.0
    val numberOfCriteria: Int = 3

    var idx: Int = 0
    var separatorsIdentical: Boolean = true
    var blockTypesIdentical: Boolean = true

    // iterate over blocks and check conditions for aligned blocks
    while (idx < split1.length && idx < split2.length) {
      val block1 = split1(idx)
      val block2 = split2(idx)


      if (!isAlphanumeric(block1) && !isAlphanumeric(block2)) {
        if (block1 != block2) {
          separatorsIdentical = false
        }
      } else {
        // checks if alphanumeric block at same position match type (number/letter)
        // TODO: check char for char because of dates which contain 1st/2nd
        if (!((isDigit(block1) && isDigit(block2)) || (isLetter(block1) && isLetter(block2)))) {
          blockTypesIdentical = false
        }
      }

      idx += 1
    }

    if (split1.length == split2.length) {
      score += 1
    }

    if (separatorsIdentical) {
      score += 1
    }

    if (blockTypesIdentical) {
      score += 1
    }

    score / numberOfCriteria
  }

  def clusterDates(dates: List[String]): mutable.Map[String, ListBuffer[List[String]]] = {
    val clusters: mutable.Map[String, ListBuffer[List[String]]] = mutable.Map()
    for (date <- dates) {
      breakable {
        for (clusterRep <- clusters.keys) {
          if (getSimilarity(clusterRep, date) == 1) {
            clusters(clusterRep).append(splitString(date))
            break
          }
        }
        clusters(date) = ListBuffer(splitString(date))
      }
    }

    clusters
  }
  // Clustering end
}
