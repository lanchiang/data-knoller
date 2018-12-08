package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultDeletePropertyImpl

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class DeleteProperty(val propertyName : String) extends Preparator {

    override def newImpl = new DefaultDeletePropertyImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {

    }
}
