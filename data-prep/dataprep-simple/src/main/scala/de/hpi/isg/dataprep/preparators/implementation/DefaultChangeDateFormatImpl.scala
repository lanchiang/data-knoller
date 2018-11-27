package de.hpi.isg.dataprep.preparators.implementation

import java.text.SimpleDateFormat

import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangeDateFormat
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/29
  */
class DefaultChangeDateFormatImpl extends PreparatorImpl {

    override protected def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
        val preparator = abstractPreparator.asInstanceOf[ChangeDateFormat]
        val propertyName = preparator.propertyName

        val rowEncoder = RowEncoder(dataFrame.schema)

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
                if (preparator.sourceDatePattern.isDefined) {
                    val sourceDatePattern = preparator.sourceDatePattern.get
                    val targetDatePattern = preparator.targetDatePattern
                    val newSeq = forepart :+ ConversionHelper.toDate(operatedValue, sourceDatePattern, targetDatePattern)
                    val newRow = Row.fromSeq(newSeq)
                    newRow
                } else {
                    val sourceDatePattern = preparator.sourceDatePattern
                    val targetDatePattern = preparator.targetDatePattern
                    val newSeq = forepart :+ formatToTargetPattern(operatedValue, targetDatePattern)
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

    def formatToTargetPattern(date: String, targetPattern: DatePatternEnum): Unit = {
        val parsedDate = toDate(date)
        new SimpleDateFormat(targetPattern.getPattern).parse(parsedDate)
    }

    def toDate(date: String): Unit = {
        for (patternMatch <- splitPattern.findFirstMatchIn(date)){
            var year: String = ""
            var month: String = ""
            var day: String = ""

            val first: String = patternMatch.group(1)
            val second: String = patternMatch.group(2)
            val third: String = patternMatch.group(3)

            println(
                s"First: $first, " +
                  s"Second: $second, " +
                  s"Third: $third"
            )

            if(first.length == 4) {
                year = first
                val result = determineDateAndMonth(second, third)
                day = result._1
                month = result._2
            } else if (third.length == 4) {
                year = third
                val result = determineDateAndMonth(first, second)
                day = result._1
                month = result._2
            } else {
                val result = handleEqualSizedBlocks(first, second, third)
                year = result._1
                month = result._2
                day = result._3
            }
            println(s"$year, $month, $day \n")
            s"$year-$month-$day"
        }
    }

    val splitPattern: Regex = "([0-9]+)[\\.\\-\\/\\s]{1}([0-9]+)[\\.\\-\\/]([0-9]+)".r

    def determineDateAndMonth(first: String, second: String): (String, String) = {
        var day: String = "-1"
        var month: String = "-1"

        if (first.toInt > 12) {
            month = first
            day = second
        } else if (second.toInt > 12) {
            day = first
            month = second
        }

        (day, month)
    }

    def handleEqualSizedBlocks(first: String, second: String, third: String): (String, String, String) = {
        var year: String = "-1"
        var month: String = "-1"
        var day: String = "-1"



        (year, month, day)
    }
}
