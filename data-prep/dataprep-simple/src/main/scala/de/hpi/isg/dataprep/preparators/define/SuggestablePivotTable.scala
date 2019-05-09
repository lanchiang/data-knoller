package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator.PreparatorTarget
import de.hpi.isg.dataprep.preparators.define.AggregationFunction.AggregationFunction
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.{Dataset, Row}

/**
  * Suggestable pivoting table preparator.
  *
  * @author Lan Jiang
  * @since 2019-05-06
  */
class SuggestablePivotTable(var rowProperty: String,
                        var columnProperty: String,
                        var valueProperty: String,
                        var aggregationFunction: AggregationFunction = AggregationFunction.Sum) extends AbstractPreparator {

  def this() {
    this(null, null, null, null)
  }

  preparatorTarget = PreparatorTarget.COLUMN_BASED

  override def buildMetadataSetup(): Unit = ???

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    // Executing a pivot table preparator requires three columns in the parameters: two for the two dimensions and the rest one for
    // the values in the pivoted table.
    // In this implementation, we stipulate that the parameter dataset must have three columns to allow a applicability calculation.
    // Among these three columns, the first stands for the rows, the second stands for the columns, and the third stands for the values.
    dataset.schema.fields.length match {
      case 3 => {
        // calculate the score only if the dataset contains three columns.
        val dataTypeArray = dataset.schema.fields
        val valueColDataType = if (dataTypeArray(2).dataType.isInstanceOf[NumericType]) 1f else 0f

        val rowCount = dataset.count().toFloat
        val distinctRatio1 = dataset.select(dataset.columns(0)).distinct().count().toFloat / rowCount
        val distinctRatio2 = dataset.select(dataset.columns(1)).distinct().count().toFloat / rowCount

        // Todo: fill the parameters
        rowProperty = dataset.columns(0)
        columnProperty = dataset.columns(1)
        valueProperty = dataset.columns(2)

        val score = (1 - distinctRatio1) * (1 - distinctRatio2) * valueColDataType
        score
      }
      case _ => 0f
    }
  }

  override def toString = s"SuggestablePivotTable($rowProperty, $columnProperty, $valueProperty, $aggregationFunction)"
}

object AggregationFunction extends Enumeration {
  type AggregationFunction = Value
  val Sum, Average, Std = Value
}