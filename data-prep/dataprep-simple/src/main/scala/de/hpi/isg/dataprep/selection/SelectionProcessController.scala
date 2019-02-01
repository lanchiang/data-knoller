package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.context.DataContext
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.selection.schema.DefaultSchemaMapping
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._

class SelectionProcessController(dataContext: DataContext, decisionEngine: DecisionEngine) {

  val allPreps: List[Preparation] = loadPreparations()

  def createPipeline(): Pipeline = {
    new Pipeline(dataContext, selectionLoop(allPreps, List[Preparation](), dataContext.getSchemaMapping, dataContext.getDataFrame, dataContext.getTargetMetadata.asScala.toList): _*)
  }


  def selectionLoop(allPreps: List[Preparation], currentPreps: List[Preparation], schemaMapping: SchemaMapping, data: Dataset[Row], metadata: List[Metadata]): List[Preparation] = {
    if (schemaMapping.hasMapped) {
      return currentPreps
    }
    val nextPrep = decisionEngine.selectNextPreparation(allPreps, schemaMapping, data, metadata)
    nextPrep match {
      case None => currentPreps
      case Some(value) => selectionLoop(allPreps, currentPreps :+ value._1, new DefaultSchemaMapping(value._2, schemaMapping.getTargetSchema), data, metadata)
    }
  }


  private def loadPreparations(): List[Preparation] = ???


}

