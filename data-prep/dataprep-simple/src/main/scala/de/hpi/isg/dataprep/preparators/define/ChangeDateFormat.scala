package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

class ChangeDateFormat(val propertyName: String,
                       val sourceDatePattern: Option[DatePatternEnum] = None,
                       val targetDatePattern: Option[DatePatternEnum] = None) extends AbstractPreparator {
  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of
    * metadata into both prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = ListBuffer[Metadata]()
    val toChange = ListBuffer[Metadata]()
    prerequisites += new PropertyDataType(propertyName, PropertyType.STRING)

    if (propertyName == null) {
      throw new ParameterNotSpecifiedException("Property name not specified!")
    }

    sourceDatePattern match {
      case Some(pattern) => prerequisites += new PropertyDatePattern(pattern, new ColumnMetadata(propertyName))
      case None =>
    }

    targetDatePattern match {
      case Some(pattern) => toChange += new PropertyDatePattern(pattern, new ColumnMetadata(propertyName))
      case None => throw new ParameterNotSpecifiedException("Target pattern not specified!")
    }

    this.prerequisites.addAll(prerequisites.toList.asJava)
    this.updates.addAll(toChange.toList.asJava)
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    0
  }
}

object ChangeDateFormat {
  def apply(
             propertyName: String,
             sourceDatePattern: DatePatternEnum,
             targetDatePattern: DatePatternEnum
           ): ChangeDateFormat = {
    new ChangeDateFormat(propertyName, Option(sourceDatePattern), Option(targetDatePattern))
  }

  def apply(propertyName: String, targetDatePattern: DatePatternEnum): ChangeDateFormat = {
    new ChangeDateFormat(propertyName, targetDatePattern = Option(targetDatePattern))
  }
}
