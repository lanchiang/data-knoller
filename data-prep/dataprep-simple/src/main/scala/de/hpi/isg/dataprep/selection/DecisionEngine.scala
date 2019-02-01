package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.model.target.system.Engine
import org.apache.spark.sql.{Dataset, Row}

abstract class DecisionEngine extends Engine {
  def selectNextPreparation(allPreps: List[Preparation], schemaMapping: SchemaMapping, data: Dataset[Row], metadata: List[Metadata]): Option[(Preparation, Schema)]
}

class SimpleDecisionEngine extends DecisionEngine {
  override def selectNextPreparation(allPreps: List[Preparation], schemaMapping: SchemaMapping, data: Dataset[Row], metadata: List[Metadata]): Option[(Preparation, Schema)] = {
    val max = allPreps.map(prep => (prep, prep.preparator.calculateApplicability(schemaMapping, data, metadata))).maxBy(_._2._1)

    if (max._2._1 > 0) {
      Some((max._1, max._2._2))
    } else {
      None
    }

  }
}