package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.preparators.implementation.DefaultExplodeArrayImpl

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
}
