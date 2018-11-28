package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitAttributeImpl

class SplitAttribute(val propertyName: String,
                     val separator: String,
                     val startLeft: Boolean,
                     val times: Int
                    ) extends Preparator{

  def this(propertyName: String, separator: String) {
    this(propertyName, separator, true, -1)
  }

  def this(propertyName: String) {
    this(propertyName, null, true, -1)
  }

  this.impl = new DefaultSplitAttributeImpl
  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {

  }
}

object SplitAttribute {}