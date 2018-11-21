package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeDateFormatImpl
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum

/**
  *
  * @author Lan Jiang
  * @since 2018/9/2
  */
class ChangeDateFormat(val propertyName : String,
                       val sourceDatePattern : DatePatternEnum,
                       val targetDatePattern: DatePatternEnum) extends Preparator {

    this.impl = new DefaultChangeDateFormatImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If it succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {
        val prerequisites = new util.ArrayList[Metadata]
        val toChange = new util.ArrayList[Metadata]

        if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("Propertry name not specified.", propertyName))
        // Trim can only be applied on String data type. Later version can support trim on other data type
        //        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

        prerequisites.add(new PropertyDatePattern(sourceDatePattern, new ColumnMetadata(propertyName)))
        toChange.add(new PropertyDatePattern(targetDatePattern, new ColumnMetadata(propertyName)))

        this.prerequisites.addAll(prerequisites)
        this.updates.addAll(toChange)
    }
}