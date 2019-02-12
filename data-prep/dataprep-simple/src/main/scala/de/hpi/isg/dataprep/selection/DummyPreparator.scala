package de.hpi.isg.dataprep.selection

import org.apache.spark.sql.{Dataset, Row}

import scala.util.Random

class DummyPreparator(name: String) {

  def this() = this("DummyPreparator")
  val impl = new DefaultDummyPreparator

  def calApplicability(dataset: Dataset[Row]): Float = {
    Random.nextFloat()
  }
}
