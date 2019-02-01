package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import org.apache.spark.sql.{Dataset, Row}

abstract class Preparator(val name: String, val preparatorConfig: PreparationConfig) {
  val prerequisites: List[Metadata] = initPrerequisites()
  val updates: List[Metadata] = initUpdates()

  def initPrerequisites(): List[Metadata]

  def initUpdates(): List[Metadata]

  def execute(input: Dataset[Row]): Dataset[Row]

  def calculateApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], sourceMetadata: List[Metadata]): (Double, Schema)
}

