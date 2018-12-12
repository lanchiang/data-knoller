package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.implementation.DefaultGenerateUnicodeImpl

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class GenerateUnicode(val propertyName: String) extends AbstractPreparator {

  //    override def newImpl = new DefaultGenerateUnicodeImpl

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {

  }
}
