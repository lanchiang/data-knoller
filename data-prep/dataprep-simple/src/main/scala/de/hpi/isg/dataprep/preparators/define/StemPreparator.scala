package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.preparators.implementation.DefaultStemPreparatorImpl
import de.hpi.isg.dataprep.util.DataType

class StemPreparator(val propertyNames : Set[String]) extends Preparator {

  this.impl = new DefaultStemPreparatorImpl

  def this(propertyName : String) {
    this(Set(propertyName))
  }

  def this(propertyNames : Array[String]) {
    this(propertyNames.toSet)
  }

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {

    propertyNames.foreach { propertyName : String =>
      if (propertyName == null)
        throw new ParameterNotSpecifiedException(String.format("ColumnMetadata name not specified."))
      this.prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
    }

  }

}