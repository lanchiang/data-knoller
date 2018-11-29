package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeEncodingImpl

/**
  *
  * @author Lan Jiang
  * @since 2018/9/2
  */
class ChangeEncoding(val propertyName : String) extends Preparator{

    this.impl = new DefaultChangeEncodingImpl

    /**
      * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws ParameterNotSpecifiedException
      */
    override def buildMetadataSetup(): Unit = {

    }
}
