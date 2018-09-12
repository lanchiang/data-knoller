package de.hpi.isg.dataprep.preparators

import java.util

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.implementation.DefaultCollapseImpl
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.objects.{Metadata, Property}
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.util.DataType

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class Collapse(val propertyName : String) extends Preparator {

    this.impl = new DefaultCollapseImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {
        val prerequisites = new util.ArrayList[Metadata]
        val tochanges = new util.ArrayList[Metadata]

        if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("%s not specified", propertyName))
        // Collapse can only be applied on String data type
//        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

        this.prerequisite.addAll(prerequisites)
        this.toChange.addAll(tochanges)
    }
}
