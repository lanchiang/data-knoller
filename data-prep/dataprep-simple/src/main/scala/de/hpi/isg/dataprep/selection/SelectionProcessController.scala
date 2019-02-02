package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.components.{Pipeline, Preparation}
import de.hpi.isg.dataprep.context.DataContext
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import org.apache.spark.sql.{Dataset, Row}

class SelectionProcessController(dataContext: DataContext, decisionEngine: DecisionEngine) extends PreparatorLoader {
  override val path: String = "de.hpi.isg.dataprep.preparators"
  val allPreps: List[Preparation] = loadPreparations()

  def createPipeline(): Pipeline = ???

  def selectionLoop(allPreps: List[Preparation], currentPreps: List[Preparation], schemaMapping: SchemaMapping, data: Dataset[Row], metadata: List[Metadata]): List[Preparation] = ???


  private def loadPreparations(): List[Preparation] = ???

}

