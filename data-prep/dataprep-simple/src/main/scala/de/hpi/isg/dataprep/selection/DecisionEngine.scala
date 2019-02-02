package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.model.target.system.{AbstractPreparator, Engine}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.Column

import scala.collection.JavaConverters._

abstract class DecisionEngine extends Engine with PreparatorLoader {

  override val path: String = "de.hpi.isg.dataprep.preparators"

  def selectNextPreparation(allPreps: List[Class[_ <: AbstractPreparator]], schemaMapping: SchemaMapping, data: Dataset[Row], metadata: List[Metadata]): Option[AbstractPreparator]
}

class SimpleDecisionEngine extends DecisionEngine {
  override def selectNextPreparation(allPreps: List[Class[_ <: AbstractPreparator]], schemaMapping: SchemaMapping, data: Dataset[Row], metadata: List[Metadata]): Option[AbstractPreparator] = {
    if (allPreps.isEmpty || schemaMapping.getCurrentSchema.getAttributes.isEmpty) return None

    //val dataSets = (1 to data.columns.length).flatMap(i => data.columns.map(new Column(_)).combinations(i)).map(cols => data.select(cols: _*))
    val dataSets = data.columns.map(new Column(_)).combinations(1).map(cols => data.select(cols: _*))
    val max = allPreps.flatMap(prepClass =>
      dataSets.map(data => {
        val prep = loadPreparator(prepClass)
        (prep, prep.calApplicability(schemaMapping, data, metadata.asJavaCollection))
      })).maxBy(_._2)

    if (max._2 > 0) {
      Some(max._1)
    } else {
      None
    }
  }
}