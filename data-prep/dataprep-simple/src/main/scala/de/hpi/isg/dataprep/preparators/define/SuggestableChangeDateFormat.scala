package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator.PreparatorTarget
import de.hpi.isg.dataprep.preparators.implementation.{ChangeDateFormatUtils, LocalePattern, PatternCriteria}
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Suggestable changing date format preparator
  *
  * @author Lan Jiang
  * @since 2019-04-23
  */
class SuggestableChangeDateFormat(var propertyName: String,
                                  var sourceDatePattern: Option[DatePatternEnum] = None,
                                  var targetDatePattern: Option[DatePatternEnum] = None) extends AbstractPreparator {

  /**
    * Empty parameter constructor for the decision engine.
    */
  def this() {
    this(null)
  }

  preparatorTarget = PreparatorTarget.COLUMN_BASED

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of
    * metadata into both prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new util.ArrayList[Metadata]
    val toChange = new util.ArrayList[Metadata]
    prerequisites.add(new PropertyDataType(propertyName, PropertyType.STRING))

    if (propertyName == null) {
      throw new ParameterNotSpecifiedException("Property name not specified!")
    }

    sourceDatePattern match {
      case Some(pattern) => prerequisites.add(new PropertyDatePattern(pattern, new ColumnMetadata(propertyName)))
      case None =>
    }

    targetDatePattern match {
      case Some(pattern) => toChange.add(new PropertyDatePattern(pattern, new ColumnMetadata(propertyName)))
      case None => throw new ParameterNotSpecifiedException("Target pattern not specified!")
    }

    this.prerequisites.addAll(prerequisites)
    this.updates.addAll(toChange)
  }

  /**
    * Calculate the applicability score of the preparator on the dataset.
    *
    * @param schemaMapping  is the schema of the input data
    * @param dataset        is the input dataset
    * @param targetMetadata is the set of { @link Metadata} that shall be fulfilled for the output data
    * @return the applicability matrix succinctly represented by a hash map. Each key stands for
    *         a { @link ColumnCombination} in the dataset, and its value the applicability score of this preparator signature.
    */
  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    alreadyApplied(targetMetadata) match {
      case true => 0f
      case false => {
        dataset.columns.length match {
          case 1 => {
            propertyName = dataset.columns.toSeq(0)
            val largestClusters: Array[(Int, (PatternCriteria, Iterable[String]))] = dataset.rdd
                    .filter(_(0) != null) // skip null
                    .map(_(0).toString) // here it implies the input data only contains one column
                    .groupBy(ChangeDateFormatUtils.getSimilarityCriteria)
                    .map(group => (group._2.size, group))
                    .takeOrdered(3)(ClusterSizeOrdering.reverse)
            val totalNumberOfValues: Float = largestClusters.map(_._1).sum
            val dateClusters = largestClusters.map(_._2._2)

            val datePatternClusters: Array[(Option[LocalePattern], List[String])] = dateClusters
                    .map(clusteredDates => (ChangeDateFormatUtils.extractClusterDatePattern(clusteredDates.toList),
                            clusteredDates.toList))

            var failedNumberOfValues = 0
            val targetPattern = DatePatternEnum.DayMonthYear // Currently Day,Month,Year is the only target.
            targetDatePattern = Option(targetPattern) // set the parameter

            for (entry <- datePatternClusters) {
              val optionLocalePattern = entry._1
              for (date <- entry._2) {
                Try{
                  ChangeDateFormatUtils.formatToTargetPattern(date, targetPattern, optionLocalePattern)
                } match {
                  case Failure(_) => failedNumberOfValues = failedNumberOfValues + 1
                  case Success(_) =>
                }
              }
            }
            // score
            1.0f - failedNumberOfValues / totalNumberOfValues
          }
          case _ => 0f // if the input dataset contains more or less than one column, do not suggest this preparator.
        }
      }
    }
  }

  /**
    * Return true if the required metadata is already in the target metadata set.
    *
    * @param targetMetadataSet is the target metadata set
    * @return true if the required metadata is already in the target metadata set
    */
  private def alreadyApplied(targetMetadataSet: util.Collection[Metadata]): Boolean = {
    updates.asScala.filter(metadata => containByValue(metadata, targetMetadataSet)).size match {
      case count if count > 0 => true
      case _ => false
    }
  }

  private def containByValue(metadata: Metadata, collection: util.Collection[Metadata]): Boolean = {
    val contain = collection.asScala.filter(target => target.equalsByValue(metadata)).size match {
      case count if count > 0 => true
      case _ => false
    }
    contain
  }

  private object ClusterSizeOrdering extends Ordering[(Int, (PatternCriteria, Iterable[String]))] {
    override def compare(x: (Int, (PatternCriteria, Iterable[String])), y: (Int, (PatternCriteria, Iterable[String]))): Int = {
      x._1.compare(y._1)
    }
  }

  override def toString = s"SuggestableChangeDateFormat($propertyName, $sourceDatePattern, $targetDatePattern)"
}