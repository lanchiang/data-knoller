package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl
import de.hpi.isg.dataprep.preparators.implementation.SplitPropertyUtils.Separator
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{DataFrame, Encoders}

class SplitProperty(var propertyName: Option[String], var separator: Option[Separator], var numCols: Option[Int], var fromLeft: Boolean) extends AbstractPreparator {
  def this(propertyName: String, separator: Separator, numCols: Int, fromLeft: Boolean = true) {
    this(Some(propertyName), Some(separator), Some(numCols), fromLeft)
  }

  def this(propertyName: String, separator: Separator) {
    this(Some(propertyName), Some(separator), None, true)
  }

  def this(propertyName: String, numCols: Int) {
    this(Some(propertyName), None, Some(numCols), true)
  }

  def this(propertyName: String) {
    this(Some(propertyName), None, None, true)
  }

  def this() {
    this(None, None, None, true)
  }

  override def buildMetadataSetup(): Unit = {
    if (this.propertyName.isEmpty)
      throw new ParameterNotSpecifiedException("propertyName not specified.")

    val propertyName = this.propertyName.get

    numCols match {
      case Some(n) =>
        if (n <= 1) throw new IllegalArgumentException("numCols must be greater than 1 in order to split a column.")
      case _ =>
    }

    prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataFrame: DataFrame, targetMetadata: util.Collection[Metadata]): Float = {
    if (dataFrame.columns.length != 1) return 0
    val propertyName = dataFrame.columns(0)
    val numCols = schemaMapping.getTargetBySourceAttributeName(propertyName).size()
    if (numCols <= 1) return 0

    val column = dataFrame.select(propertyName).as(Encoders.STRING)
    val splitPropertyImpl = impl.asInstanceOf[DefaultSplitPropertyImpl]
    val separator = splitPropertyImpl.findSeparator(column, numCols)

    this.numCols = Some(numCols)
    this.separator = Some(separator)
    this.propertyName = Some(propertyName)
    splitPropertyImpl.evaluateSplit(column, separator, numCols)
  }
}
