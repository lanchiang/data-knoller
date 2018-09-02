package de.hpi.isg.dataprep.preparators

import java.util
import java.util.{ArrayList, List}

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.implementation.DefaultReplaceSubstringImpl
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.`object`.Metadata
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.util.DataType

/**
  *
  * @author Lan Jiang
  * @since 2018/9/2
  */
class ReplaceSubstring(val propertyName : String,
                       val source : String,
                       val replacement : String,
                       val times : Int) extends Preparator {

    this.impl = new DefaultReplaceSubstringImpl

    def this(propertyName :String, source : String, replacement : String) {
        this(propertyName, source, replacement, ReplaceSubstring.DEFAULT_TIMES)
    }

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {
        val prerequisites = new util.ArrayList[Metadata]
        val tochange = new util.ArrayList[Metadata]

        if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("Property name not specified"))
        if (source == null) throw new ParameterNotSpecifiedException(String.format("Source sub-string not specified"))
        if (replacement == null) throw new ParameterNotSpecifiedException(String.format("Target sub-string not specified"))
        if (times < 0) throw new IllegalArgumentException(String.format("Cannot replace the first minus sub-strings."))

        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

        this.prerequisite.addAll(prerequisites)
        this.toChange.addAll(tochange)
    }
}

object ReplaceSubstring {

    private val DEFAULT_TIMES = 0 // if default_times is used. Replace all the found source string with replacement.
}
