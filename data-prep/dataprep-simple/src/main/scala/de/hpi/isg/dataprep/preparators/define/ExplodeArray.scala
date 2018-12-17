package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.preparators.implementation.DefaultExplodeArrayImpl
import org.apache.spark.sql.{Dataset, Row}

class ExplodeArray(val propertyName: String, val columnNames: Option[Array[String]] = None) extends AbstractPreparator {

  def this(propertyName: String, columnNames: Array[String]) {
    this(propertyName, Some(columnNames))
  }

  def this(propertyName: String) {
    this(propertyName, None)
  }


  this.impl = new DefaultExplodeArrayImpl

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {

  }

  override def calApplicability(dataset: Dataset[Row], sourceSchema: Schema, targetSchema: Schema, targetMetadata: util.Collection[Metadata]): Float = {
    0
  }
}
