package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.implementation.DefaultRenamePropertyImpl

/**
  *
  * @author Lan Jiang
  * @since 2018/9/2
  */
class RenameProperty(val propertyName: String,
                     val newPropertyName: String) extends AbstractPreparator {

  //    override def newImpl = new DefaultRenamePropertyImpl

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {

  }
}
