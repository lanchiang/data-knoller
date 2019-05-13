package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import org.apache.spark.sql.{Dataset, Row}

import collection.JavaConverters._

/**
  * The suggestable transpose preparator that flip the column and row of a table.
  *
  * @author Lan Jiang
  * @since 2019-05-09
  */
class SuggestableTranspose extends AbstractPreparator {
  override def buildMetadataSetup(): Unit = ???

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata], pipeline: AbstractPipeline): Float = {
    ???
  }

  override def getAffectedProperties: util.List[String] = {
    List.empty[String].asJava
  }
}
