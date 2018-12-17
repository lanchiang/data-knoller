package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}


class SplitProperty(val propertyName: String, val separator: Option[String], val numCols: Option[Int], val fromLeft: Boolean) extends AbstractPreparator {
  impl = new DefaultSplitPropertyImpl

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

  /**
    * Calculate the matrix of preparator applicability to the data. In the matrix, each
    * row represent a specific signature of the preparator, while each column represent a specific
    * {@link ColumnCombination} of the data
    *
    * @return the applicability matrix succinctly represented by a hash map. Each key stands for
    *         a { @link ColumnCombination} in the dataset, and its value the applicability score of this preparator signature.
    */
  override def calApplicability(dataset: Dataset[Row], sourceSchema: Schema, targetSchema: Schema, targetMetadata: util.Collection[Metadata]): util.Map[ColumnCombination, lang.Float] = {
    null
  }
}
