package de.hpi.isg.dataprep.preparators.implementation

import java.text.SimpleDateFormat
import java.util.Date

import de.hpi.isg.dataprep.{ConversionHelper, DateScorer, ExecutionContext}
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangeDateFormat
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.rdd.RDD
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

    // since we only work on string columns and output strings we can use the input schema
    val rowEncoder = RowEncoder(dataFrame.schema)

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

  /**
    * The date conversion uses a combination of regexes and a deep learning model. It takes the most frequent matching
    * strict and fuzzy regex and applies them according to our presented approach (first strict, if it doesnt work
    * and the score is high enough then the fuzzy regex).
    *
    * @param dataset the dataset that is preparated
    * @param rowEncoder rowencoder used to work on the dataset
    * @param errorAccumulator collects thrown errors
    * @param fieldName the field that this preparator works on
    * @param targetPattern the target date pattern to which the dates are converted
    * @return dataset containing the converted column
    */
  def convertDateInDataset(
                            dataset: Dataset[Row],
                            rowEncoder: ExpressionEncoder[Row],
                            errorAccumulator: CollectionAccumulator[PreparationError],
                            fieldName: String,
                            targetPattern: DatePatternEnum
                          ): Dataset[Row] = {
    val strictRegex = getMostMatchedRegex(dataset.rdd.map(_.getAs[String](fieldName)), fuzzy = false)
    // for the conversion we use the fuzzy regex with the most matches (unlike the scoring)
    val fuzzyRegex = getMostMatchedRegex(dataset.rdd.map(_.getAs[String](fieldName)), fuzzy = true)
    val scorer = new DateScorer()
    val threshold = 0.5f

    dataset.flatMap { row =>
      val index = row.fieldIndex(fieldName)
      val seq = row.toSeq
      val forepart = seq.take(index)
      val backpart = seq.takeRight(row.length - index - 1)
      val date = row.getAs[String](fieldName)

      // convert first with the strict regex and if that doesn't work and the score is high enough with the fuzzy regex
      var convertedDate: String = null
      var error: Exception = null
      try {
        convertedDate = toDate(date, targetPattern, strictRegex)
      } catch {
        case e: Exception => error = e
      }
      if (convertedDate == null && scorer.score(date) >= threshold) {
        try {
          convertedDate = toDate(date, targetPattern, fuzzyRegex)
        } catch {
          case e: Exception => error = e
        }
      }

      // if the converted date is null then neither regex could extract a date
      convertedDate match {
        case null =>
          errorAccumulator.add(new RecordError(date, error))
          None
        case conversionResult =>
          val newSeq = (forepart :+ conversionResult) ++ backpart
          val newRow = Row.fromSeq(newSeq)
          Option(newRow)
      }
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

  def getMostMatchedRegex(dates: RDD[String], fuzzy: Boolean): DateRegex = {
    val regexValidCount = dates
      .flatMap { date =>
        DateRegex.allRegexes(fuzzy).map { regex =>
          val isValid = regex.validMatch(date)
          val count = if (isValid) 1 else 0
          (regex, count)
        }
      }.reduceByKey(_ + _)
      .collect
      .toList

    // get the tuple with the most matches and select the key of that entry (the regex)
    regexValidCount.maxBy(_._2)._1
  }

  /**
    * This scoring is the implementation of our presented approach. It uses two different kinds of regexes and a keras
    * deep learning model that is wrapped in the DateScorer class.
    * The strict regex that is used is the one with the most matches over the whole columns (in regards to the order of
    * day, month and year). For the fuzzy regex we try out every combination of the order since its purpose is
    * validating the score of the deep learning model (the model can handle any of the orders of day, month and year).
    *
    * @param date the date that is scored
    * @param scorer wrapper of the deep learning model
    * @param strictRegex the strict regex with the most matches over the dataset
    * @return the resulting score of the date (either 1.0 if it is a date or 0.0 if it isn't)
    */
  def scoreDate(date: String, scorer: DateScorer, strictRegex: DateRegex): Float = {
    val targetPattern = DatePatternEnum.DayMonthYear
    val threshold = 0.5f
    val fuzzyRegexes = DateRegex.allRegexes(fuzzy = true)

    // apply the strict regex
    try {
      toDate(date, targetPattern, strictRegex)
      return 1.0f
    } catch {
      // an exception is thrown if the date is not parseable
      case e: Exception =>
    }

    // if the strict regex does not match score the date and possibly apply the fuzzy regex
    val score = scorer.score(date)
    // scores below the threshold mean that the input is not a date
    if (score < threshold) {
      return 0.0f
    }

    // if any of the fuzzy regexes match the maximum in the result list will be 1.0 (otherwise 0.0)
    val fuzzyScores = fuzzyRegexes.map { regex =>
      try {
        toDate(date, targetPattern, regex)
        1.0f
      } catch {
        case e: Exception => 0.0f
      }
    }
    fuzzyScores.max
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

  def apply(regexIndex: Int, fuzzy: Boolean = false): DateRegex = regexIndex match {
    case 0 =>
      // match any 4 digit year, only 1-12 and 01-12 for the month and 1-31 and 01-31 for the day
      // format DD-MM-YYYY
      DateRegex(
        makeRegex(List(dayPattern, monthPattern, yearPattern), fuzzy),
        dayIndex = 1,
        monthIndex = 2,
        yearIndex = 3)
    case 1 =>
      // match any 4 digit year, only 1-12 and 01-12 for the month and 1-31 and 01-31 for the day
      // format MM-DD-YYYY
      DateRegex(
        makeRegex(List(monthPattern, dayPattern, yearPattern), fuzzy),
        dayIndex = 2,
        monthIndex = 1,
        yearIndex = 3)
    case 2 =>
      // match any 4 digit year, only 1-12 and 01-12 for the month and 1-31 and 01-31 for the day
      // format YYYY-MM-DD
      DateRegex(
        makeRegex(List(yearPattern, monthPattern, dayPattern), fuzzy),
        dayIndex = 3,
        monthIndex = 2,
        yearIndex = 1)
    case 3 =>
      // match any 4 digit year, only 1-12 and 01-12 for the month and 1-31 and 01-31 for the day
      // format YYYY-DD-MM
      DateRegex(
        makeRegex(List(yearPattern, dayPattern, monthPattern), fuzzy),
        dayIndex = 2,
        monthIndex = 3,
        yearIndex = 1)
    case _ => throw new IllegalArgumentException("There are no more regexes")
  }

  private def makeRegex(pattern: List[String], fuzzy: Boolean): Regex = {
    if (fuzzy) {
      s".*?${pattern.mkString(".*?")}.*?".r
    } else {
      pattern.mkString(".?").r
    }
  }

  def allRegexes(fuzzy: Boolean = false): List[DateRegex] = (0 to 3).map(this.apply(_, fuzzy)).toList
}
