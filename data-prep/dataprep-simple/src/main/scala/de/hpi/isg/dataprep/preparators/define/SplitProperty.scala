package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

class SplitProperty(val propertyName: String, val separator: Option[String], val numCols: Option[Int], val fromLeft: Boolean) extends AbstractPreparator {
  private val splitPropertyImpl = this.impl.asInstanceOf[DefaultSplitPropertyImpl]

  def this(propertyName: String, separator: String, numCols: Int, fromLeft: Boolean = true) {
    this(propertyName, Some(separator), Some(numCols), fromLeft)
  }

  def this(propertyName: String, separator: String) {
    this(propertyName, Some(separator), None, true)
  }

  def this(propertyName: String) {
    this(propertyName, None, None, true)
  }

  override def buildMetadataSetup(): Unit = {
    if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("%s not specified.", propertyName))

    numCols match {
      case Some(n) =>
        if (n <= 1) throw new IllegalArgumentException("numCols must be greater than 1 in order to split a column.")
      case _ =>
    }

    this.prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    import dataset.sparkSession.implicits._

    if (!dataset.columns.contains(propertyName)) return 0
    val numCols = schemaMapping.getTargetBySourceAttributeName(propertyName).size()
    if (numCols <= 1) return 0

    val separator = this.separator.getOrElse(this.splitPropertyImpl.findSeparator(dataset, propertyName))
    val column = dataset.select(propertyName).map(row => row.getString(0))

    this.splitPropertyImpl.evaluateSplit(column, separator, numCols)
  }
}
