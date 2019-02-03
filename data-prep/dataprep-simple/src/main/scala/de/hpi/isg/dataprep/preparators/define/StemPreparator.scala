package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.preparators.implementation.DefaultStemPreparatorImpl
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

class StemPreparator(val propertyNames: Set[String]) extends AbstractPreparator {

  this.impl = new DefaultStemPreparatorImpl

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
      throw new ParameterNotSpecifiedException(
        String.format("ColumnMetadata name not specified.")
      )
    propertyNames.foreach { propertyName: String =>
      if (propertyName == null)
        throw new ParameterNotSpecifiedException(
          String.format("ColumnMetadata name not specified.")
        )
      this.prerequisites.add(
        new PropertyDataType(propertyName, DataType.PropertyType.STRING)
      )
    }

  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    0
  }

}