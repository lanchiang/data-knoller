package de.hpi.isg.dataprep.preparators.implementation

import java.text.SimpleDateFormat
import java.util.Date

import de.hpi.isg.dataprep.{ConversionHelper, ExecutionContext}
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl

import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.define.ChangeDateFormat
import de.hpi.isg.dataprep.schema.SchemaUtils
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

class DefaultChangeDateFormatImpl extends AbstractPreparatorImpl with Serializable {
  /**
    * The abstract class of preparator implementation.
    *
    * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the
    *                           corresponding subclass in the implementation body.
    * @param dataFrame          contains the intermediate dataset
    * @param errorAccumulator   is the { @link CollectionAccumulator} to store preparation errors while executing the
    *                           preparator.
    * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
    * @throws Exception
    */
  override protected def executeLogic(
                                       abstractPreparator: AbstractPreparator,
                                       dataFrame: Dataset[Row],
                                       errorAccumulator: CollectionAccumulator[PreparationError]
                                     ): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[ChangeDateFormat]
    val fieldName = preparator.propertyName
    // Here the program needs to check the existence of these fields.

    val rowEncoder = RowEncoder(SchemaUtils.updateSchema(
      dataFrame.schema,
      fieldName,
      DataType.getSparkTypeFromInnerType(PropertyType.STRING)))

    val targetPattern = preparator.targetDatePattern.get
    val sourcePatternOpt = preparator.sourceDatePattern

    val createdDataset = sourcePatternOpt match {
      case Some(pattern) => convertDateInDataset(dataFrame, rowEncoder, errorAccumulator, fieldName, pattern, targetPattern)
      case None => convertDateInDataset(dataFrame, rowEncoder, errorAccumulator, fieldName, targetPattern)
    }

    createdDataset.persist()

    createdDataset.count()

    new ExecutionContext(createdDataset, errorAccumulator)
  }


  def convertDateInDataset(
                            dataset: Dataset[Row],
                            rowEncoder: ExpressionEncoder[Row],
                            errorAccumulator: CollectionAccumulator[PreparationError],
                            fieldName: String,
                            sourcePattern: DatePatternEnum,
                            targetPattern: DatePatternEnum
                          ): Dataset[Row] = {
    dataset.flatMap { row =>
      val index = row.fieldIndex(fieldName)
      val seq = row.toSeq
      val forepart = seq.take(index)
      val backpart = seq.takeRight(row.length - index - 1)

      val tryRow = Try {
        val convertedValue = ConversionHelper.toDate(row.getAs[String](fieldName), sourcePattern, targetPattern)

        val newSeq = (forepart :+ convertedValue) ++ backpart
        val newRow = Row.fromSeq(newSeq)
        newRow
      }
      val trial = tryRow match {
        case Failure(content) =>
          errorAccumulator.add(new RecordError(row.getAs[String](fieldName), content))
          tryRow
        case Success(_) => tryRow
      }
      trial.toOption
    }(rowEncoder)
  }

  def convertDateInDataset(
                            dataset: Dataset[Row],
                            rowEncoder: ExpressionEncoder[Row],
                            errorAccumulator: CollectionAccumulator[PreparationError],
                            fieldName: String,
                            targetPattern: DatePatternEnum
                          ): Dataset[Row] = {
    val regexValidCount = dataset.rdd
      .flatMap { row =>
        val date = row.getAs[String](fieldName)
        DateRegex.allRegexes.map { regex =>
          val isValid = regex.validMatch(date)
          val count = if (isValid) 1 else 0
          (regex, count)
        }
      }.reduceByKey(_ + _)
      .collect
      .toList
    val mostMatchedRegex = regexValidCount.maxBy(_._2)._1

    dataset.flatMap { row =>
      val index = row.fieldIndex(fieldName)
      val seq = row.toSeq
      val forepart = seq.take(index)
      val backpart = seq.takeRight(row.length - index - 1)

      val tryRow = Try {
        val convertedValue = toDate(row.getAs[String](fieldName), targetPattern, mostMatchedRegex)

        val newSeq = (forepart :+ convertedValue) ++ backpart
        val newRow = Row.fromSeq(newSeq)
        newRow
      }
      val trial = tryRow match {
        case Failure(content) =>
          errorAccumulator.add(new RecordError(row.getAs[String](fieldName), content))
          tryRow
        case Success(_) => tryRow
      }
      trial.toOption
    }(rowEncoder)
  }

  /**
    * Converts the pattern of the date value into the desired pattern.
    *
    * @param value  the value to be converted
    * @param target the target date pattern.
    * @return the converted [[Date]]
    */
  @throws(classOf[Exception])
  def toDate(value: String, target: DatePatternEnum, dateRegex: DateRegex): String = {
    if (!dateRegex.validMatch(value)) {
      throw new IllegalArgumentException(s"""Unparseable date: "$value"""")
    }

    val regexMatch = dateRegex.regex.findAllMatchIn(value).next()

    val days = regexMatch.group(dateRegex.dayIndex)
    val dayPattern = "d" * days.length

    val months = regexMatch.group(dateRegex.monthIndex)
    val monthPattern = "M" * months.length

    val years = regexMatch.group(dateRegex.yearIndex)
    val yearPattern = "y" * years.length

    val date = s"$years-$months-$days"
    val parsedDate = new SimpleDateFormat(s"$yearPattern-$monthPattern-$dayPattern").parse(date)
    new SimpleDateFormat(target.getPattern).format(parsedDate)
  }
}

case class DateRegex(regex: Regex, yearIndex: Int, monthIndex: Int, dayIndex: Int) {
  def validMatch(date: String): Boolean = {
    val matches = regex.findAllMatchIn(date)
    if (matches.isEmpty) {
      // no match
      return false
    }
    val regexMatch = matches.next()
    // the date is only valid if the whole string was matched
    regexMatch.group(0).length == date.length
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: DateRegex => regex.toString == other.regex.toString
      case _ => false
    }
  }

  override def hashCode(): Int = {
    regex.toString.hashCode
  }
}

object DateRegex {
  val dayPattern = "(3[01]|[1-2][0-9]|0?[1-9])"
  val monthPattern = "(1[012]|0?[1-9])"
  val yearPattern = "(\\d{4})"

  def apply(regexIndex: Int): DateRegex = regexIndex match {
    case 0 =>
      // match any 4 digit year, only 1-12 and 01-12 for the month and 1-31 and 01-31 for the day
      // format DD-MM-YYYY
      DateRegex(
        s"$dayPattern.*?$monthPattern.*?$yearPattern".r,
        dayIndex = 1,
        monthIndex = 2,
        yearIndex = 3)
    case 1 =>
      // match any 4 digit year, only 1-12 and 01-12 for the month and 1-31 and 01-31 for the day
      // format MM-DD-YYYY
      DateRegex(
        s"$monthPattern.*?$dayPattern.*?$yearPattern".r,
        dayIndex = 2,
        monthIndex = 1,
        yearIndex = 3)
    case 2 =>
      // match any 4 digit year, only 1-12 and 01-12 for the month and 1-31 and 01-31 for the day
      // format YYYY-MM-DD
      DateRegex(
        s"$yearPattern.*?$monthPattern.*?$dayPattern".r,
        dayIndex = 3,
        monthIndex = 2,
        yearIndex = 1)
    case 3 =>
      // match any 4 digit year, only 1-12 and 01-12 for the month and 1-31 and 01-31 for the day
      // format YYYY-DD-MM
      DateRegex(
        s"$yearPattern.*?$dayPattern.*?$monthPattern".r,
        dayIndex = 2,
        monthIndex = 3,
        yearIndex = 1)
    case _ => throw new IllegalArgumentException("There are no more regexes")
  }

  def allRegexes: List[DateRegex] = (0 to 3).map(this.apply).toList
}
