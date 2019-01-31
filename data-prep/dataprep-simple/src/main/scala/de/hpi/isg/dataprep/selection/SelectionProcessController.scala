package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import org.apache.spark.sql.DataFrame

class SelectionProcessController(dataFrame: DataFrame, schemaMapping: SchemaMapping, metaData: Metadata, decisionEngine: DecisionEngine) {

  def createPipeline(): Pipeline = ???
}

