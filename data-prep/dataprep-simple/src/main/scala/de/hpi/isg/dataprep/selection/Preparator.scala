package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.objects.Metadata
import org.apache.spark.sql.{Dataset, Row}

abstract class Preparator(val name: String, val preparatorConfig: PreparatorConfig) {
  val prerequisites: List[Metadata] = initPrerequisites()
  val updates: List[Metadata] = initUpdates()

  def initPrerequisites(): List[Metadata]

  def initUpdates(): List[Metadata]

  def execute(input: Dataset[Row]): Dataset[Row]
}
