package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.Metadata
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.model.repository.MetadataRepository
import de.hpi.isg.dataprep.model.target.objects.MetadataOld
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitAttributeImpl
import org.apache.spark.sql.{Dataset, Row}

class SplitAttribute(val propertyName: String,
                     val separator: String,
                     val startLeft: Boolean,
                     val times: Int
                    ) extends AbstractPreparator {

  def this(propertyName: String, separator: String) {
    this(propertyName, separator, true, -1)
  }

  def this(propertyName: String) {
    this(propertyName, null, true, -1)
  }

  this.impl = new DefaultSplitAttributeImpl

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {

  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata], pipeline: AbstractPipeline): Float = 0
}

object SplitAttribute {}