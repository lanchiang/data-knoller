package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl.{MultiValueCharacterClassSeparator, MultiValueStringSeparator, SingleValueCharacterClassSeparator, SingleValueStringSeparator}
import de.hpi.isg.dataprep.preparators.implementation.SplitPropertyUtils.Separator
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.CollectionAccumulator

object DefaultSplitPropertyImpl {

  case class SingleValueStringSeparator(separatorValue: String) extends Separator {
    override def getNumSplits(value: String): Int = {
      val result = value.sliding(separatorValue.length).count(_ == separatorValue) + 1
      result
    }

    override def executeSplit(value: String): Vector[String] =
      value.split(separatorValue).toVector

    override def merge(split: Vector[String], original: String): String =
      split.mkString(separatorValue)

    override def mostLikelySeparator: String = separatorValue
  }

  case class MultiValueStringSeparator(numCols: Int, globalDistribution: Map[String, Int]) extends Separator {
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

    override def mostLikelySeparator: String = globalDistribution.maxBy(_._2)._1
  }

  case class SingleValueCharacterClassSeparator(separatorValue: String) extends Separator {
    override def getNumSplits(value: String): Int =
      SplitPropertyUtils.toCharacterClasses(value)._1.sliding(separatorValue.length).count(_ == separatorValue) + 1

    override def executeSplit(value: String): Vector[String] =
      SplitPropertyUtils.addSplitteratorBetweenCharacterTransition(value, separatorValue)
        .split(SplitPropertyUtils.defaultSplitterator).toVector

    override def merge(split: Vector[String], original: String): String =
      split.mkString("")

    override def mostLikelySeparator: String = separatorValue
  }

  case class MultiValueCharacterClassSeparator(numCols: Int, globalDistribution: Map[String, Int]) extends Separator {
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

    override def mostLikelySeparator: String = globalDistribution.maxBy(_._2)._1
  }

}

class DefaultSplitPropertyImpl extends AbstractPreparatorImpl {
  override def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val parameters = abstractPreparator.asInstanceOf[SplitProperty]

    if (parameters.propertyName.isEmpty)
      throw new ParameterNotSpecifiedException(String.format("%s not specified.", "propertyName"))

    val resultDataFrame = try {
      executeSplitProperty(dataFrame, parameters)
    } catch {
      case e: Throwable =>
        errorAccumulator.add(new RecordError(parameters.propertyName.get, e))
        dataFrame
    }
    new ExecutionContext(resultDataFrame, errorAccumulator)
  }

  def executeSplitProperty(dataFrame: DataFrame, parameters: SplitProperty): DataFrame = {
    val propertyName = parameters.propertyName.get
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
    val separatorCandidates =
      stringSeparatorDistribution.keys.map(SingleValueStringSeparator) ++
        characterClassDistribution.keys.map(SingleValueCharacterClassSeparator)

    separatorCandidates.maxBy(evaluateSplit(column, _))
  }

  def findSeparator(column: Dataset[String], numCols: Int): Separator = {
    val stringSeparatorDistribution = SplitPropertyUtils.globalStringSeparatorDistribution(column, numCols)
    val characterClassDistribution = SplitPropertyUtils.globalTransitionSeparatorDistribution(column, numCols)
    val separatorCandidates =
      List(
        MultiValueStringSeparator(numCols, stringSeparatorDistribution),
        MultiValueCharacterClassSeparator(numCols, characterClassDistribution)
      ) ++
        stringSeparatorDistribution.keys.map(SingleValueStringSeparator) ++
        characterClassDistribution.keys.map(SingleValueCharacterClassSeparator)

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
      .maxBy { case (_, numOccurrences) => numOccurrences }
      ._1
  }

  def evaluateSplit(column: Dataset[String], separator: Separator): Float = {
    import column.sparkSession.implicits._
    val result = column
      .map(separator.getNumSplits)
      .groupByKey(identity)
      .mapGroups { case (_, occurrences) => occurrences.length }
      .collect.max.toFloat / column.count()
    result
  }

  def evaluateSplit(column: Dataset[String], separator: Separator, numCols: Int): Float = {
    import column.sparkSession.implicits._
    val expectedNumSplits = numCols - 1
    column
      .map(separator.getNumSplits(_) - 1)
      .map(numSplits => expectedNumSplits - Math.abs(expectedNumSplits - numSplits))
      .map(rowScore => if (rowScore < 0) 0 else rowScore)
      .collect.sum.toFloat / expectedNumSplits / column.count()
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
