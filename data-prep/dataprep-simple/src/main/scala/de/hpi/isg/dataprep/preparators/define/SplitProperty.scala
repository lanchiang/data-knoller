package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl
import de.hpi.isg.dataprep.util.DataType


class SplitProperty(val propertyName: String, val separator: String, val fromLeft: Boolean = true, val times: Option[Int]) extends Preparator{

    impl = new DefaultSplitPropertyImpl

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new util.ArrayList[Metadata]
    if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("%s not specified.", propertyName))

    times match {
      case Some(t) => {
        if (t <= 1) throw new IllegalArgumentException("Times must be greater than 1 in order to split a column.")
      }
      case _ =>
    }
    prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

    this.prerequisites.addAll(prerequisites)
  }
}
