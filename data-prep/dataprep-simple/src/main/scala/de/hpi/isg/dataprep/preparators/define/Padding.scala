package de.hpi.isg.dataprep.preparators.define

import java.util
import java.util.{ArrayList, List}

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.preparators.implementation.DefaultPaddingImpl
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DataType.PropertyType

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class Padding(val propertyName : String,
              val expectedLength : Int,
              val padder : String) extends Preparator {

    def this(propertyName : String, expectedLength : Int) {
        this(propertyName, expectedLength, Padding.DEFAULT_PADDER)
    }

    this.impl = new DefaultPaddingImpl
    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {
        val prerequisites = new util.ArrayList[Metadata]
        val tochange = new util.ArrayList[Metadata]

        if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("%s not specified.", propertyName))
        // illegal padding length was input.
        if (expectedLength <= 0) throw new IllegalArgumentException(String.format("Padding length is illegal!"))

        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

        // when basic statistics is implemented, one shall be capable of retrieving value length from the metadata repository
        // therefore, this method shall compare the value length as well.

        this.prerequisites.addAll(prerequisites)
        this.updates.addAll(tochange)
    }
}

object Padding {

    val DEFAULT_PADDER = "0"
}