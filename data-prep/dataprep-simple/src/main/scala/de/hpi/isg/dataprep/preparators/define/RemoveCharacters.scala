package de.hpi.isg.dataprep.preparators.define

import java.util
import java.util.{ArrayList, List}

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.preparators.implementation.DefaultRemoveCharactersImpl
import de.hpi.isg.dataprep.util.{DataType, RemoveCharactersMode}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class RemoveCharacters(val propertyName : String,
                       val mode : RemoveCharactersMode,
                       val userSpecifiedCharacters : String) extends Preparator{

    def this(propertyName : String, mode : RemoveCharactersMode) {
        this(propertyName, mode, "")
    }

//    override def newImpl = new DefaultRemoveCharactersImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {
        val prerequisites = new util.ArrayList[Metadata]
        val tochange = new util.ArrayList[Metadata]

        if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("ColumnMetadata name not specified."))
        else if (mode == null) throw new ParameterNotSpecifiedException(String.format("Remove character mode not specified."))

//        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

        if (mode eq RemoveCharactersMode.CUSTOM) if (userSpecifiedCharacters == null) throw new ParameterNotSpecifiedException(String.format("Characters must be specified if choosing custom mode."))

        this.prerequisites.addAll(prerequisites)
        this.updates.addAll(tochange)
    }
}
