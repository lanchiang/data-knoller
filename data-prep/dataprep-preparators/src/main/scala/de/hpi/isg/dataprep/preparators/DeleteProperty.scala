package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.implementation.DefaultDeletePropertyImpl
import de.hpi.isg.dataprep.model.target.preparator.Preparator

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class DeleteProperty(val propertyName : String) extends Preparator {

    this.impl = new DefaultDeletePropertyImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {

    }
}
