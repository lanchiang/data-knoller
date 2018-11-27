package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

class DefaultSplitPropertyImpl extends PreparatorImpl {

  override def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[SplitProperty]
    val propertyName = preparator.propertyName
    val separator = preparator.separator
    val fromLeft = preparator.fromLeft
    val times = preparator.times

    val resultDataFrame = appendEmptyColumns(dataFrame, propertyName, times)
    val rowEncoder = RowEncoder(resultDataFrame.schema)

    val splitPropertyDataset = dataFrame.map(
      row => splitRow(row, propertyName, separator, times)
    )(rowEncoder)

    splitPropertyDataset.count()
    new ExecutionContext(splitPropertyDataset, errorAccumulator)
  }

  def splitRow(row: Row, propertyName: String, separator: String, times: Int): Row = {
    val index = row.fieldIndex(propertyName)
    val value = row.getAs[String](index)
    val splitValues = value.split(separator, times)
    val fill = List.fill(times - splitValues.length)("")

    Row.fromSeq(row.toSeq ++ splitValues ++ fill)
  }

  def appendEmptyColumns(dataSet: Dataset[Row], propertyName: String, n: Int): Dataset[Row] = {
    if (!dataSet.schema.contains(propertyName)) {
      throw new IllegalArgumentException(s"dataFrame has no column $propertyName")
    }

    var result = dataSet
    for (i <- 1 to n) {
      result = result.withColumn(s"$propertyName$i", lit(""))
    }
    result
  }
}
