package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator.PreparatorTarget
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters

/**
  * @author Lan Jiang
  * @since 2019-05-09
  */
class SuggestableDeleteProperty(var propertyName: String) extends AbstractPreparator {

  def this() {
    this(null)
  }

  preparatorTarget = PreparatorTarget.COLUMN_BASED

  override def buildMetadataSetup(): Unit = ???

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata], pipeline: AbstractPipeline): Float = {
    val score = dataset.columns.size match {
      case 1 => {
        propertyName = dataset.columns(0)

        val numOfNull = dataset.filter(row => row.isNullAt(0)).count()
        val totalNum = dataset.count()
        val nullRatio = numOfNull.toFloat / totalNum.toFloat

        val retrospectivePreparators = pipeline.getPipelineExecutionHistory
        val numOfThisPropertyPresence = JavaConverters.asScalaIteratorConverter(retrospectivePreparators.iterator()).asScala
                .filter(preparator => preparator.getAffectedProperties.contains(propertyName))
                .toSeq.size

        val numOfRetrospectivePreparators = retrospectivePreparators.size
        val retrospectivePresenceScore = numOfThisPropertyPresence.toFloat / numOfRetrospectivePreparators.toFloat
        nullRatio * retrospectivePresenceScore
      }
      case _ => 0f // if the input set does not contain one and only one column, delete property is invalid.
    }
    score
  }

  override def getAffectedProperties: util.List[String] = {
    convertToPropertyList(propertyName)
  }


  override def toString = s"SuggestableDeleteProperty($propertyName)"
}
