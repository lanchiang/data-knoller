package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{DINPhoneNumber, PropertyDataType}
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

class ChangePhoneFormat(val propertyName: String,
                        val sourceFormat: DINPhoneNumber,
                        val targetFormat: DINPhoneNumber) extends AbstractPreparator {

  def this(propertyName: String, targetFormat: DINPhoneNumber) {
    this(propertyName, null, targetFormat)
  }

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    if (propertyName == null) throw new ParameterNotSpecifiedException("Property name must be specified.")
    if (targetFormat == null) throw new ParameterNotSpecifiedException("Target format must be specified.")

    this.prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    0
  }
}