package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl.{CharacterClassSeparatorNoColNum, CharacterClassSeparatorWithColNum, MultiValueSeparator, SingleValueSeparator}
import de.hpi.isg.dataprep.preparators.implementation.SplitPropertyUtils.Separator
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
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
        .maxBy { case (candidate, _) => globalDistribution(candidate) }

      value.split(separatorValue).toVector
    }

    override def merge(split: Vector[String], original: String): String = {
      val (separatorValue, _) = SplitPropertyUtils
        .bestStringSeparatorCandidates(original, numCols)
        .maxBy { case (candidate, _) => globalDistribution(candidate) }

      split.mkString(separatorValue)
    }
  }

  case class CharacterClassSeparatorNoColNum(globalDistribution: Map[String, Int]) extends Separator {
    override def getNumSplits(value: String): Int = {
      val (_, count) = SplitPropertyUtils.allCharacterClassCandidates(value)(0)
      count
    }

    override def executeSplit(value: String): Vector[String] = {
      val (separatorValue, _) = SplitPropertyUtils
        .allCharacterClassCandidates(value)
        .maxBy { case (candidate, _) => globalDistribution(candidate) }

      SplitPropertyUtils.addSplitteratorBetweenCharacterTransition(value, separatorValue)
        .split(SplitPropertyUtils.defaultSplitterator).toVector
    }

    override def merge(split: Vector[String], original: String): String = {
      split.mkString("")
    }
  }

  case class CharacterClassSeparatorWithColNum(numCols: Int, globalDistribution: Map[String, Int]) extends Separator {
    override def getNumSplits(value: String): Int = {
      val (_, count) = SplitPropertyUtils.bestCharacterClassSeparatorCandidates(value, numCols)(0)
      count
    }

    override def executeSplit(value: String): Vector[String] = {
      val (separatorValue, _) = SplitPropertyUtils
        .bestCharacterClassSeparatorCandidates(value, numCols)
        .maxBy { case (candidate, _) => globalDistribution(candidate) }

      SplitPropertyUtils.addSplitteratorBetweenCharacterTransition(value, separatorValue)
        .split(SplitPropertyUtils.defaultSplitterator).toVector
    }

    override def merge(split: Vector[String], original: String): String = {
      split.mkString("")
    }
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
    val column = dataFrame.select(propertyName).as(Encoders.STRING)

    val (separator, numCols) = (parameters.separator, parameters.numCols) match {
      case (None, None) =>
        val sep = findSeparator(column)
        val num = findNumCols(column, sep)
        (sep, num)
      case (Some(sep), None) =>
        val num = findNumCols(column, sep)
        (sep, num)
      case (None, Some(num)) =>
        val sep = findSeparator(column, num)
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

  def findSeparator(column: Dataset[String]): Separator = {
    val stringSeparatorDistribution = SplitPropertyUtils.globalStringSeparatorDistribution(column)
    val characterClassDistribution = SplitPropertyUtils.globalTransitionSeparatorDistribution(column)
    val separatorCandidates = CharacterClassSeparatorNoColNum(characterClassDistribution) ::
      stringSeparatorDistribution.keys.map(SingleValueSeparator).toList

    separatorCandidates.maxBy(evaluateSplit(column, _))
  }

  def findSeparator(column: Dataset[String], numCols: Int): Separator = {
    val stringSeparatorDistribution = SplitPropertyUtils.globalStringSeparatorDistribution(column, numCols)
    val characterClassDistribution = SplitPropertyUtils.globalTransitionSeparatorDistribution(column, numCols)
    val separatorCandidates = MultiValueSeparator(numCols, stringSeparatorDistribution) ::
        CharacterClassSeparatorWithColNum(numCols, characterClassDistribution) ::
        stringSeparatorDistribution.keys.map(SingleValueSeparator).toList

    separatorCandidates.maxBy(evaluateSplit(column, _, numCols))
  }

  def findNumCols(column: Dataset[String], separator: Separator): Int = {
    // Given a column a separator, this method returns the number of columns that should be created by a split.
    // This number is the most common number of splits created by this separator throughout all rows
    import column.sparkSession.implicits._
    val numsSplits = column
      .map(separator.getNumSplits)
      .filter(x => x > 1)

    if (numsSplits.count() == 0)
      throw new IllegalArgumentException(s"Separator could not split any value.")

    numsSplits
      .groupByKey(identity)
      .mapGroups { case (numSplits, occurrences) => (numSplits, occurrences.length) }
      .collect
      .maxBy{case (_, numOccurrences) => numOccurrences}
      ._1
  }

  def evaluateSplit(column: Dataset[String], separator: Separator): Float = {
    import column.sparkSession.implicits._
    column
      .map(separator.getNumSplits)
      .groupByKey(identity)
      .mapGroups { case (_, occurrences) => occurrences.length }
      .collect.max.toFloat / column.count()
  }

  def evaluateSplit(column: Dataset[String], separator: Separator, numCols: Int): Float = {
    import column.sparkSession.implicits._
    column
      .map(separator.getNumSplits)
      .map(numSplits => numCols - Math.abs(numCols - numSplits))
      .map(rowScore => if (rowScore < 0) 0 else rowScore)
      .collect.sum.toFloat / numCols / column.count()
  }

  def createSplitValuesDataFrame(dataFrame: DataFrame, propertyName: String, separator: Separator, times: Int, fromLeft: Boolean): Dataset[Row] = {
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

  def appendEmptyColumns(dataFrame: DataFrame, propertyName: String, numCols: Int): Dataset[Row] = {
    var result = dataFrame
    for (i <- 1 to numCols) {
      result = result.withColumn(s"$propertyName$i", lit(""))
    }
    result
  }
}
