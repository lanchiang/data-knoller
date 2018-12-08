package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultRenamePropertyImpl

/**
  *
  * @author Lan Jiang
  * @since 2018/9/2
  */
class RenameProperty(val propertyName : String,
                     val newPropertyName : String) extends Preparator {

//    override def newImpl = new DefaultRenamePropertyImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {

    }
}
