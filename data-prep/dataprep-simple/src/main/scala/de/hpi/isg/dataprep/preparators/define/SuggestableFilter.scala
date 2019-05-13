package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator.PreparatorTarget
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import de.hpi.isg.dataprep.preparators.define.FilterStrategy.FilterStrategy
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.{Dataset, Row}

import collection.JavaConverters._

/**
  * The suggestable filter preparator that removes unnecessary rows.
  *
  * @author Lan Jiang
  * @since 2019-05-13
  */
class SuggestableFilter(var propertyNames: Array[String], var strategy: FilterStrategy = FilterStrategy.OutlierDetection) extends AbstractPreparator {

  def this() {
    this(null)
  }

  preparatorTarget = PreparatorTarget.ROW_BASED

  override def buildMetadataSetup(): Unit = ???

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata], pipeline: AbstractPipeline): Float = {
    val score = strategy match {
          case FilterStrategy.OutlierDetection => {
            propertyNames = dataset.columns
            propertyNames.length match {
              case 1 => {
                val propertyName = propertyNames(0)
                dataset.schema(propertyName).dataType match {
                  case dataType if dataType.isInstanceOf[NumericType] => {
                    val Array(q1, q3) = dataset.stat.approxQuantile(propertyName, Array(0.25, 0.75), 0.0)

                    val IQR = q3 - q1
                    val low = q1 - 1.5 * IQR
                    val up = q3 + 1.5 * IQR

                    import dataset.sparkSession.implicits._

                    // The more outlier gets detected, the higher score filtering gets.
                    dataset.map(row => row.getAs[Double](propertyName))
                            .filter(value => value < low || value > up)
                            .count().toFloat / dataset.count().toFloat
                  }
                  case _ => 0f
                }
              }
              case _ => 0f
            }
          }
          case _ => 0f
        }
    score
  }

  override def getAffectedProperties: util.List[String] = {
    List.empty[String].asJava
  }
}

object FilterStrategy extends Enumeration {
  type FilterStrategy = Value
  val OutlierDetection = Value
}