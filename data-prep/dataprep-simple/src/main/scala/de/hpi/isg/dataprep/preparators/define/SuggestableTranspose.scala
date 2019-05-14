package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.Metadata
import de.hpi.isg.dataprep.model.target.objects.MetadataOld
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator.PreparatorTarget
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

  preparatorTarget = PreparatorTarget.TABLE_BASED

  override def buildMetadataSetup(): Unit = ???

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata], pipeline: AbstractPipeline): Float = {
    // Todo: if the table is a pivoted table, we may suggest to transpose it
    isPivotTable(dataset) match {
      case false => 0f
      case true => {
        1f
      }
    }
  }

  def isPivotTable(dataset: Dataset[Row]): Boolean = {
    false
  }

  override def getAffectedProperties: util.List[String] = {
    List.empty[String].asJava
  }
}
