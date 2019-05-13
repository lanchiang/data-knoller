package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.repository.MetadataRepository
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeDelimiterImpl
import org.apache.spark.sql.{Dataset, Row}

/**
  * @author Lan Jiang
  * @since 2018/9/17
  */
class ChangeDelimiter(val tableName: String,
                      val sourceDelimiter: String,
                      val targetDelimiter: String) extends AbstractPreparator {

  //    override def newImpl = new DefaultChangeDelimiterImpl

  def this(tableName: String, targetDelimiter: String) {
    this(tableName, null, targetDelimiter)
  }

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata], pipeline: AbstractPipeline): Float = 0
}
