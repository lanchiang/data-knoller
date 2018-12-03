package de.hpi.isg.dataprep.preparators.implementation

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangeDateFormat
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.control.Breaks._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/29
  */
class DefaultChangeDateFormatImpl extends PreparatorImpl with Serializable {

    override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
        val preparator = abstractPreparator.asInstanceOf[ChangeDateFormat]
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
                case Failure(content) => {
                    throw content
                }
                case Success(content) => {
                    content
                }
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
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setLenient(false)
        dateFormat.format(d)
    }

    def convertStringToDate(s: String, pattern: String): Date = {
        val dateFormat = new SimpleDateFormat(pattern)
        dateFormat.setLenient(false)
        dateFormat.parse(s)
    }

    def formatToTargetPattern(date: String, targetPattern: DatePatternEnum, extractedRegex: Map[String, Long]): String = {
        for((pattern, count) <- extractedRegex) {
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

    def toPattern(date: String): Option[String] = {
        val splitPattern: Regex = "([0-9]+)([\\.\\-\\/\\s]{1})([0-9]+)([\\.\\-\\/]{1})([0-9]+)".r

        for (patternMatch <- splitPattern.findFirstMatchIn(date)){
            var year: String = "XXXX"
            var month: String = "XX"
            var day: String = "XX"
            val groups = padSingleDigitDates(patternMatch.subgroups)
            val numGroups = List(groups.head, groups(2), groups(4))
            val separators = List(groups(1), groups(3))

            //print("Numbers:", numGroups)
            //println("Sep:", separators)

            numGroups.find { group =>
                group.length == 4
            } match {
                case Some(group) =>
                    year = group
                    val result = determineDateAndMonth(numGroups.filter(_ != group))
                    day = result._1
                    month = result._2
                case None =>
                    val result = handleEqualSizedBlocks(numGroups)
                    year = result._1
                    month = result._2
                    day = result._3
            }

            if (year != "XXXX" && month != "XX" && day != "XX") {
                val result = generatePatternAndRegex(groups, separators, year, month, day)
                //println("Pattern: " + result._2 + " Regex: " + result._1)
                year = padYearIfNeeded(year)
                return Option(result._2)
            }
            //println(s"Date: $date")
            //println(s"YYYY-MM-DD: $year-$month-$day")
            //println(s"--------------------------")
            return None
        }
        None
    }

    def generatePlaceholder(origString:String, placeholder:String):String = {
        origString.replaceAll(".", placeholder)
    }

    def generatePatternAndRegex(fullGroup:List[String], separators:List[String], year: String, month: String, day: String): (String, String) = {
        var newGroup = fullGroup.updated(fullGroup.indexOf(year), generatePlaceholder(year, "y"))
        newGroup = newGroup.updated(newGroup.indexOf(month), generatePlaceholder(month, "M"))
        newGroup = newGroup.updated(newGroup.indexOf(day), generatePlaceholder(day, "d"))

        var pattern: String = ""
        var regex: String = ""

        for(group <- newGroup) {
            val groupAsString = group.toString

            pattern = pattern + groupAsString

            "[DMY]+".r.findFirstMatchIn(groupAsString) match {
                case Some(_) =>
                    regex = regex + s"[0-9]{${groupAsString.length}}"
                case None =>
                    regex = regex + "\\" + groupAsString // TODO: better escaping
            }

        }

        (regex, pattern)
    }

    def determineDateAndMonth(groups: List[String]): (String, String) = {
        var day: String = "XX"
        var month: String = "XX"

        groups.find { group =>
            group.toInt > 12
        } match {
            case Some(group) =>
                day = group
                month = groups.filter(_ != group).head
            case None =>
        }

        (day, month)
    }

    def handleEqualSizedBlocks(groups: List[String]): (String, String, String) = {
        var year: String = "XXXX"
        var month: String = "XX"
        var day: String = "XX"

        groups.find { group =>
            group.toInt > 31
        } match {
            case Some(group) =>
                year = group
                val result = determineDateAndMonth(groups.filter(_ != group))
                month = result._1
                day = result._2
            case None =>
        }

        (year, month, day)
    }

    def padYearIfNeeded(year: String): String = {
        val currentYear: Int = 18 //TODO compute
        var paddedYear: String = year
        // The idea is kinda stupid because it won't allow dates which lie in the future
        // However, this leads to the problem that there will no way of determining the full year if only 2 digits are given
        if(year.length == 2 && year.toInt > currentYear) {
            paddedYear = "19" + year
        }

        paddedYear
    }

    def padSingleDigitDates(dates: List[String]): List[String] = {
        dates.map( date => if (date.length == 1 && date.forall(Character.isDigit)) "0" + date else date )
    }
}
