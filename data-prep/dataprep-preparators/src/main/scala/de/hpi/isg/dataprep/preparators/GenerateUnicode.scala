package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.implementation.DefaultGenerateUnicodeImpl
import de.hpi.isg.dataprep.model.target.preparator.Preparator

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class GenerateUnicode(val propertyName :String) extends Preparator {

    this.impl = new DefaultGenerateUnicodeImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {

    }
}
