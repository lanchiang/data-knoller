package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Encoder, Row}
import org.apache.spark.util.CollectionAccumulator

class DefaultSplitPropertyImpl extends PreparatorImpl {

  override def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[SplitProperty]
    val propertyName = preparator.propertyName

    val (separator, numCols) = try {
      val separator = preparator.separator match {
        case Some(s) => s
        case _ => findSeparator(dataFrame, propertyName)
      }

      val numCols = preparator.numCols match {
        case Some(n) => n
        case _ => findNumberOfColumns(dataFrame, propertyName, separator)
      }

      (separator, numCols)
    } catch {
      case e: IllegalArgumentException =>
        errorAccumulator.add(new RecordError(propertyName, e))
        return new ExecutionContext(dataFrame, errorAccumulator)
    }

    val splitValuesDataFrame = createSplitValuesDataFrame(
      dataFrame,
      propertyName,
      separator,
      numCols,
      preparator.fromLeft
    )

    splitValuesDataFrame.count()
    new ExecutionContext(splitValuesDataFrame, errorAccumulator)
  }

  def findSeparator(dataFrame: Dataset[Row], propertyName: String): String = {
    // Given a column name, this method returns the character that is most likely the separator.
    // Possible separators are non-alphanumeric characters that are evenly distributed over all rows.
    // Rows where the character does not appear at all are ignored.
    // If multiple characters fulfill this condition, the most common character is selected.

    if (!dataFrame.columns.contains(propertyName))
      throw new IllegalArgumentException(s"No column $propertyName found!")

    val charMaps = dataFrame.select(propertyName).collect().map(
      row => {
        val value = row.getAs[String](0)
        value.groupBy(identity).filter{
          case(char, string) => !char.isLetterOrDigit
        }.mapValues(_.length)
      })
    val chars = charMaps.flatMap(map => map.keys).distinct

    val checkSeparatorCondition = (char: Char) => {
      val counts = charMaps.map(map => map.withDefaultValue(0)(char)).filter(x => x > 0)
      (counts.forall(_ == counts.head), counts.head, char)
    }
    val candidates = chars.map(checkSeparatorCondition).filter{case (valid, counts, char) => valid}

    if (candidates.isEmpty)
      throw new IllegalArgumentException(s"No possible separator found in column $propertyName")
    candidates.maxBy{case (valid, counts, char) => counts}._3.toString
  }

  def findNumberOfColumns(dataFrame: Dataset[Row], propertyName: String, separator: String): Int = {
    // Given a column name, and a separator, this method returns the number of columns that should be
    // created by a split. This number is the maximum of the number of split parts over all rows.

    val counts = dataFrame.select(propertyName).collect().map(
      row => {
        val value = row.getAs[String](0)
        value.split(separator).length
      }
    ).filter(x => x > 1)

    if (counts.isEmpty)
      throw new IllegalArgumentException(s"Separator not found in column $propertyName")
    counts.max
  }

  def createSplitValuesDataFrame(dataFrame: Dataset[Row], propertyName: String, separator: String, times: Int, fromLeft: Boolean): Dataset[Row] = {
    if (!dataFrame.columns.contains(propertyName))
      throw new IllegalArgumentException(s"No column $propertyName found!")

    val splitValue = (value: String) => {
      var split = value.split(separator)
      if (!fromLeft)
        split = split.reverse

      if (split.length <= times) {
        val fill = List.fill(times - split.length)("")
        split = split ++ fill
      }

      val head = split.slice(0, times - 1)
      var tail = split.slice(times - 1, split.length)
      if (!fromLeft) {
        tail = tail.reverse
      }
      head :+ tail.mkString(separator)
    }

    val rowEncoder: Encoder[Row] = RowEncoder(appendEmptyColumns(dataFrame, propertyName, times).schema)
    dataFrame.map(
      row => {
        val index = row.fieldIndex(propertyName)
        val value = row.getAs[String](index)
        val split = splitValue(value)
        Row.fromSeq(row.toSeq ++ split)
      }
    )(rowEncoder)
  }

  def appendEmptyColumns(dataFrame: Dataset[Row], propertyName: String, numCols: Int): Dataset[Row] = {
    var result = dataFrame
    for (i <- 1 to numCols) {
      result = result.withColumn(s"$propertyName$i", lit(""))
    }
    result
  }
}
