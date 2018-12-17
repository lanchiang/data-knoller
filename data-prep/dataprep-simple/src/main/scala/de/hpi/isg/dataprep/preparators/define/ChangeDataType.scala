package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeDataTypeImpl
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.ListBuffer
import collection.JavaConversions._

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class ChangeDataType(val propertyName: String,
                     val sourceType: PropertyType,
                     val targetType: PropertyType,
                     val sourceDatePattern: DatePatternEnum,
                     val targetDatePattern: DatePatternEnum) extends AbstractPreparator {

  def this(propertyName: String, targetType: PropertyType) {
    this(propertyName, null, targetType = targetType, null, null)
  }

  def this(propertyName: String, sourceType: PropertyType, targetType: PropertyType) {
    this(propertyName, sourceType, targetType, null, null)
  }

  def this(propertyName: String, targetType: PropertyType,
           sourceDatePattern: DatePatternEnum, targetDatePattern: DatePatternEnum) {
    this(propertyName, null, targetType, sourceDatePattern, targetDatePattern)
  }

  //    override def newImpl = new DefaultChangeDataTypeImpl

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new ListBuffer[Metadata]
    val toChange = new ListBuffer[Metadata]

    if (propertyName == null) {
      throw new ParameterNotSpecifiedException("Property name not specified!")
    }
    if (targetType == null) {
      throw new ParameterNotSpecifiedException("Target data type not specified!")
    } else {
      toChange.add(new PropertyDataType(propertyName, targetType))
    }

    if (sourceType != null) {
      prerequisites += new PropertyDataType(propertyName, sourceType)
    }
    else {
      // use the value in the metadata repository.
      val metadataRepository = this.getPreparation.getPipeline.getMetadataRepository
      val sourceTypeMetadata = new PropertyDataType(propertyName, sourceType)
      if (metadataRepository.getMetadataPool.contains(sourceTypeMetadata)) {
        prerequisites += metadataRepository.getMetadata(sourceTypeMetadata);
      }
    }

    if (targetType == DataType.PropertyType.DATE) {
      if (sourceDatePattern != null) {
        prerequisites += new PropertyDatePattern(sourceDatePattern, new ColumnMetadata(propertyName))
      }
      else {
        // use the value in the metadata repository.
        val metadataRepository = this.getPreparation.getPipeline.getMetadataRepository
        val sourceDatePatternMetadata = new PropertyDatePattern(sourceDatePattern, new ColumnMetadata(propertyName))
        if (metadataRepository.getMetadataPool.contains(sourceDatePatternMetadata)) {
          prerequisites += sourceDatePatternMetadata
        }
      }
      if (targetDatePattern == null) {
        throw new ParameterNotSpecifiedException("Change to DATE type but target date pattern not specified!")
      }
      else {
        toChange.add(new PropertyDatePattern(targetDatePattern, new ColumnMetadata(propertyName)))
      }
    }

    this.prerequisites.addAll(prerequisites.toList)
    this.updates.addAll(toChange.toList)
  }

  override def calApplicability(dataset: Dataset[Row],
                                sourceSchema: Schema,
                                targetSchema: Schema,
                                targetMetadata: util.Collection[Metadata]): Float = {
    0
  }
}
