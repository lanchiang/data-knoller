package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.preparators.implementation.DefaultLemmatizePreparatorImpl
import de.hpi.isg.dataprep.util.DataType

class LemmatizePreparator(val propertyName : String) extends Preparator {

  this.impl = new DefaultLemmatizePreparatorImpl

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {

    if (propertyName == null)
      throw new ParameterNotSpecifiedException(String.format("ColumnMetadata name not specified."))

    this.prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

  }

}