package de.hpi.isg.dataprep.preparators

import java.util

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultDummyPreparatorImpl
import org.apache.spark.sql.{Dataset, Row}

import scala.util.Random

class DummyPreparator(name: String) extends AbstractPreparator {

  override def buildMetadataSetup(): Unit = ???

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = Random.nextFloat()

  def this() = this("DummyPreparator")


  this.impl = new DefaultDummyPreparatorImpl

}
