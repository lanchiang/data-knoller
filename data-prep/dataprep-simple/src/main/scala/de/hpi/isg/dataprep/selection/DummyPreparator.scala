package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import org.apache.spark.sql.{Dataset, Row}

import scala.util.Random

class DummyPreparator {
  val impl = new DefaultDummyPreparator

  def calApplicability(dataset: Dataset[Row]): Float = {
    Random.nextFloat()
  }
}
