package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator

class ChangePhoneFormat(val propertyName : String,
                        val sourceFormat : String,
                        val targetFormat : String) extends Preparator {

  def this(propertyName : String, targetFormat : String) {
    this(propertyName, null, targetFormat)
  }
  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
  }
}