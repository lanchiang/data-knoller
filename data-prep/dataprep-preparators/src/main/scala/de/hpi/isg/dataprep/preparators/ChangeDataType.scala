package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.implementation.DefaultChangeDataTypeImpl
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.objects.{Metadata, Property}
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import collection.JavaConversions._

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class ChangeDataType(val propertyName : String,
                     val sourceType : PropertyType,
                     val targetType : PropertyType,
                     val sourceDatePattern : DatePatternEnum,
                     val targetDatePattern : DatePatternEnum) extends Preparator {

    this.impl = new DefaultChangeDataTypeImpl

    def this(propertyName : String, targetType : PropertyType) {
        this(propertyName, null, targetType = targetType, null, null)
    }

    def this(propertyName : String, sourceType : PropertyType, targetType : PropertyType) {
        this(propertyName, sourceType, targetType, null, null)
    }

    def this(propertyName : String, targetType : PropertyType,
             sourceDatePattern : DatePatternEnum, targetDatePattern : DatePatternEnum) {
        this(propertyName, null, targetType, sourceDatePattern, targetDatePattern)
    }

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {
        var prerequisites = new ListBuffer[Metadata]()
        var tochanges = new ListBuffer[Metadata]()

        if (propertyName == null) throw new ParameterNotSpecifiedException("Property name not specified!")
        if (targetType == null) throw new ParameterNotSpecifiedException("Target data type not specified!")
//        else tochanges.add(new PropertyDataType(propertyName, targetType))
        else tochanges.add(new PropertyDataType(new Property(propertyName), targetType))

//        if (sourceType != null) prerequisites += new PropertyDataType(propertyName, sourceType)
        if (sourceType != null) prerequisites += new PropertyDataType(new Property(propertyName), sourceType)
        else {
            // use the value in the metadata repository.
        }

        if (targetType == DataType.PropertyType.DATE) {
            if (sourceDatePattern != null) prerequisites += new PropertyDatePattern(new Property(propertyName), sourceDatePattern)
            else {
                // use the value in the metadata repository.
            }
            if (targetDatePattern == null) throw new ParameterNotSpecifiedException("Change to DATE type but target date pattern not specified!")
            else tochanges.add(new PropertyDatePattern(new Property(propertyName), targetDatePattern))
        }

        this.prerequisite.addAll(prerequisites.toList)
        this.toChange.addAll(tochanges.toList)
    }
}
