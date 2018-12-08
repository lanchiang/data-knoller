package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultGenerateUnicodeImpl

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class GenerateUnicode(val propertyName :String) extends Preparator {

//    override def newImpl = new DefaultGenerateUnicodeImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {

    }
}
