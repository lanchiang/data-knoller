package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.preparators.implementation.DefaultLemmatizePreparatorImpl
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

class LemmatizePreparator(val propertyNames: Set[String]) extends AbstractPreparator {

  this.impl = new DefaultLemmatizePreparatorImpl

  def this(propertyName: String) {
    this(Set(propertyName))
  }

  def this(propertyNames: Array[String]) {
    this(propertyNames.toSet)
  }

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {

    if (propertyNames == null)
      throw new ParameterNotSpecifiedException(String.format("ColumnMetadata name not specified."))
    propertyNames.foreach { propertyName: String =>
      if (propertyName == null)
        throw new ParameterNotSpecifiedException(String.format("ColumnMetadata name not specified."))
      this.prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
    }

  }

  /**
    * Calculate the matrix of preparator applicability to the data. In the matrix, each
    * row represent a specific signature of the preparator, while each column represent a specific
    * {@link ColumnCombination} of the data
    *
    * @return the applicability matrix succinctly represented by a hash map. Each key stands for
    *         a { @link ColumnCombination} in the dataset, and its value the applicability score of this preparator signature.
    */
  override def calApplicability(dataset: Dataset[Row], sourceSchema: Schema, targetSchema: Schema, targetMetadata: util.Collection[Metadata]): Float = {
    null
  }
}