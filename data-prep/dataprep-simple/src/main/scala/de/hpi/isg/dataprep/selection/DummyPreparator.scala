package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import org.apache.spark.sql.{Dataset, Row}
import java.util

import scala.util.Random

class DummyPreparator {
  val impl = new DefaultDummyPreparator

  def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    Random.nextFloat()
  }
}
