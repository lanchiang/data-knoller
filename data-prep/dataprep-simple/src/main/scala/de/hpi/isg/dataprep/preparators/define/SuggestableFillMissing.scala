package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import org.apache.spark.sql.{Dataset, Row}

/**
  * The suggestable fill missing value preparator
  *
  * @author Lan Jiang
  * @since 2019-05-13
  */
class SuggestableFillMissing(var propertyName: String, var filler: String = "default") extends AbstractPreparator {

  def this() {
    this(null)
  }

  override def buildMetadataSetup(): Unit = ???

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata], pipeline: AbstractPipeline): Float = {
    dataset.columns.length match {
      case 1 => {
        propertyName = dataset.columns(0)
        dataset.filter(row => row.isNullAt(0)).count().toFloat / dataset.count()
      }
      case _ => 0f
    }
  }
}
