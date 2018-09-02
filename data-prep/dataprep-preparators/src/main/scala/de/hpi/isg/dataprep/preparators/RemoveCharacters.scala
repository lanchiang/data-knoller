package de.hpi.isg.dataprep.preparators

import java.util
import java.util.{ArrayList, List}

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.implementation.DefaultRemoveCharactersImpl
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.Metadata
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.util.{DataType, RemoveCharactersMode}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class RemoveCharacters(val propertyName : String,
                       val userSpecifiedCharacters : String,
                       val mode : RemoveCharactersMode) extends Preparator{

    def this(propertyName : String, mode : RemoveCharactersMode) {
        this(propertyName, "" , mode)
    }

    this.impl = new DefaultRemoveCharactersImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {
        val prerequisites = new util.ArrayList[Metadata]
        val tochange = new util.ArrayList[Metadata]

        if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("Property name not specified."))
        else if (mode == null) throw new ParameterNotSpecifiedException(String.format("Remove character mode not specified."))

        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

        if (mode eq RemoveCharactersMode.CUSTOM) if (userSpecifiedCharacters == null) throw new ParameterNotSpecifiedException(String.format("Characters must be specified if choosing custom mode."))

        this.prerequisite.addAll(prerequisites)
        this.toChange.addAll(tochange)
    }
}
