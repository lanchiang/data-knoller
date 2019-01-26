package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl.SingleValueSeparator
import de.hpi.isg.dataprep.preparators.implementation.SplitPropertyUtils.Separator
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable.ListBuffer

object DefaultSplitPropertyImpl {

  case class SingleValueSeparator(separatorValue: String) extends Separator {
    override def getNumSplits(value: String): Int =
      value.sliding(separatorValue.length).count(_ == separatorValue) + 1

    override def executeSplit(value: String): Vector[String] =
      value.split(separatorValue).toVector

    override def merge(split: Vector[String], original: String): String =
      split.mkString(separatorValue)
  }

  case class MultiValueSeparator(numCols: Int, globalDistribution: Map[String, Int]) extends Separator {
    override def getNumSplits(value: String): Int = {
      val (_, count) = SplitPropertyUtils.bestStringSeparatorCandidates(value, numCols)(0)
      count
    }

    override def executeSplit(value: String): Vector[String] = {
      val (separatorValue, _) = SplitPropertyUtils
        .bestStringSeparatorCandidates(value, numCols)
        .maxBy { case (candidate, count) => globalDistribution(candidate) }

      value.split(separatorValue).toVector
    }

    override def merge(split: Vector[String], original: String): String = {
      val (separatorValue, _) = SplitPropertyUtils
        .bestStringSeparatorCandidates(original, numCols)
        .maxBy { case (candidate, count) => globalDistribution(candidate) }

      split.mkString(separatorValue)
    }
  }

  case class CharacterClassSeparator(left: Char, right: Char) extends Separator {
    override def getNumSplits(value: String): Int = ???

    override def executeSplit(value: String): Vector[String] = ???

    override def merge(split: Vector[String], original: String): String = ???
  }

}

class DefaultSplitPropertyImpl extends AbstractPreparatorImpl {
  override def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val parameters = abstractPreparator.asInstanceOf[SplitProperty]
    val resultDataFrame = try {
      executeSplitProperty(dataFrame, parameters)
    } catch {
      case e: Throwable =>
        errorAccumulator.add(new RecordError(parameters.propertyName, e))
        dataFrame
    }
    new ExecutionContext(resultDataFrame, errorAccumulator)
  }

  def executeSplitProperty(dataFrame: DataFrame, parameters: SplitProperty): DataFrame = {
    val propertyName = parameters.propertyName
    if (!dataFrame.columns.contains(propertyName))
      throw new IllegalArgumentException(s"No column $propertyName found!")

    val (separator, numCols) = (parameters.separator, parameters.numCols) match {
      case (None, None) =>
        val sep = findSeparator(dataFrame, propertyName)
        val num = findNumCols(dataFrame, propertyName, sep)
        (sep, num)
      case (Some(sep), None) =>
        val num = findNumCols(dataFrame, propertyName, sep)
        (sep, num)
      case (None, Some(num)) =>
        val sep = findSeparator(dataFrame, propertyName, num)
        (sep, num)
      case (Some(sep), Some(num)) =>
        (sep, num)
    }

    createSplitValuesDataFrame(
      dataFrame,
      propertyName,
      separator,
      numCols,
      parameters.fromLeft
    )
  }

  def findSeparator(dataFrame: DataFrame, propertyName: String): Separator = {
    // Given a column name, this method returns the character that is most likely the separator.
    // Possible separators are non-alphanumeric characters that are evenly distributed over all rows.
    // Rows where the character does not appear at all are ignored.
    // If multiple characters fulfill this condition, the most common character is selected.
    // TODO: Implement multi-char separator
    val charMaps = dataFrame.select(propertyName).collect().map(
      row => {
        val value = row.getAs[String](0)
        value.groupBy(identity).filter {
          case (char, string) => !char.isLetterOrDigit
        }.mapValues(_.length)
      })
    val chars = charMaps.flatMap(map => map.keys).distinct

    val checkSeparatorCondition = (char: Char) => {
      val counts = charMaps.map(map => map.withDefaultValue(0)(char)).filter(x => x > 0)
      (counts.forall(_ == counts.head), counts.head, char)
    }
    val candidates = chars.map(checkSeparatorCondition).filter { case (valid, _, _) => valid }

    if (candidates.isEmpty)
      throw new IllegalArgumentException(s"No possible separator found in column $propertyName")

    val separatorValue = candidates.maxBy { case (_, counts, _) => counts }._3.toString
    SingleValueSeparator(separatorValue)
  }

  def findSeparator(dataFrame: DataFrame, propertyName: String, numCols: Int): Separator = ???

  def findNumCols(dataFrame: DataFrame, propertyName: String, separator: Separator): Int = {
    // Given a column name, and a separator, this method returns the number of columns that should be
    // created by a split. This number is the maximum of the number of split parts over all rows.

    val counts = dataFrame
      .select(propertyName)
      .collect()
      .map(row =>
        separator.getNumSplits(row.getAs[String](0))
      )
      .filter(x => x > 1)

    if (counts.isEmpty)
      throw new IllegalArgumentException(s"Separator not found in column $propertyName")
    counts.max
  }

  def createSplitValuesDataFrame(dataFrame: Dataset[Row], propertyName: String, separator: Separator, times: Int, fromLeft: Boolean): Dataset[Row] = {
    implicit val rowEncoder: Encoder[Row] = RowEncoder(appendEmptyColumns(dataFrame, propertyName, times).schema)
    dataFrame.map(
      row => {
        val index = row.fieldIndex(propertyName)
        val value = row.getAs[String](index)
        val split = separator.split(value, times, fromLeft)
        Row.fromSeq(row.toSeq ++ split)
      }
    )
  }

  def appendEmptyColumns(dataFrame: Dataset[Row], propertyName: String, numCols: Int): Dataset[Row] = {
    var result = dataFrame
    for (i <- 1 to numCols) {
      result = result.withColumn(s"$propertyName$i", lit(""))
    }
    result
  }

  def evaluateSplit(column: Dataset[String], separator: Separator, numCols: Int): Float = {
    import column.sparkSession.implicits._
    column
      .map(separator.getNumSplits)
      .map(numSplits => numCols - Math.abs(numCols - numSplits))
      .map(rowScore => if (rowScore < 0) 0 else rowScore)
      .collect()
      .sum.toFloat / numCols / column.count()
  }
  
  /*
  Maps each character of the input string to its character class: letter (a), digit (1), whitespace (s) or special sign (the character itself)
   */

  def toCharacterClasses(input: String): Tuple2[String, String] = {
    val characterClasses = input.map(c => {
      if (Character.isDigit(c)) '1'
      else if (Character.isUpperCase(c)) 'a'
      else if (Character.isLowerCase(c)) 'a'
      else if (Character.isWhitespace(c)) 's'
      else '.' //c //
    })


    (characterClasses, input)
  }

  /*
  Removes duplicated characters in a row, e.g. fooboo becomes fobo
   */
  def reduceCharacterClasses(input: Tuple2[String, String]): Tuple3[String, String, String] = {
    var last = ' '
    val reduced = input._1.map(c => {
      var mapped = '\0'
      if (last != c) mapped = c
      last = c
      mapped
    })


    (reduced, input._1, input._2)
  }

  /*
  extracts all seperations which can used either used as splitt candidates or resulting splitt elements
   */
  def extractSeperatorCandidates(input: Tuple3[String, String, String]): List[String] = {

    var erg = new ListBuffer[String]
    var candidates = input._1.slice(1, input._1.length).distinct.toList

    if (candidates.isEmpty) {
      val start = getOriginCharacters(input._1.charAt(0), input)
      val end = getOriginCharacters(input._1.charAt(input._1.length - 1), input)

      start.foreach(str => {
        erg += str
      })

      end.foreach(str => {
        erg += str
      })
    }
    else {
      candidates + input._1.charAt(input._1.length - 1).toString

      candidates.foreach(candidate => {
        val elems = getOriginCharacters(candidate, input)
        elems.foreach(str => {
          erg += str
        })
      })
    }

    erg.toList
  }

  /*
  returns the origin sequence for mapped and reduced string
   */
  def getOriginCharacters(input: Char, array: Tuple3[String, String, String]): Set[String] = {

    var results = Set[String]()
    val intermediate = array._2
    val origin = array._3

    var index = intermediate.indexOf(input)
    var allidx = new ListBuffer[Integer]
    while (index >= 0) {
      allidx += index
      index = intermediate.indexOf(input, index + 1)
    }

    var allidxs = allidx.toList


    var erg = ""

    var block = false
    var i = 0
    while (i < origin.length) {
      if (allidx.contains(i)) {
        erg += origin.charAt(i)
        if (i == origin.length - 1) results += erg
      }
      else {
        block = false
        if (erg.equals("") == false) results += erg
        erg = ""
      }


      i += 1;
      i - 1
    }

    results
  }

  def getCandidates(input: String): List[String] = {
    val res = filterFirstAndLastPartOut(extractSeperatorCandidates(reduceCharacterClasses(toCharacterClasses(input))), input)
    res
  }

  def getParts(input: String): List[String] = {
    val res = extractSeperatorCandidates(reduceCharacterClasses(toCharacterClasses(input)))
    res
  }

  def filterFirstAndLastPartOut(candidates: List[String], input: String): List[String] = {

    candidates.filter(candidate => {
      input.startsWith(candidate) == false && input.endsWith(candidate) == false
    })

  }


}
