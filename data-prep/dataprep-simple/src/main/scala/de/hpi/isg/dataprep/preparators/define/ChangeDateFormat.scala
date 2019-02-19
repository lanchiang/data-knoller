package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.DateFormatScorer
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.preparators.implementation.{DateRegex, DefaultChangeDateFormatImpl}
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

class ChangeDateFormat(val propertyName: String,
                       val sourceDatePattern: Option[DatePatternEnum] = None,
                       val targetDatePattern: Option[DatePatternEnum] = None) extends AbstractPreparator {
  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of
    * metadata into both prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = ListBuffer[Metadata]()
    val toChange = ListBuffer[Metadata]()
    prerequisites += new PropertyDataType(propertyName, PropertyType.STRING)

    if (propertyName == null) {
      throw new ParameterNotSpecifiedException("Property name not specified!")
    }

    sourceDatePattern match {
      case Some(pattern) => prerequisites += new PropertyDatePattern(pattern, new ColumnMetadata(propertyName))
      case None =>
    }

    targetDatePattern match {
      case Some(pattern) => toChange += new PropertyDatePattern(pattern, new ColumnMetadata(propertyName))
      case None => throw new ParameterNotSpecifiedException("Target pattern not specified!")
    }

    this.prerequisites.addAll(prerequisites.toList.asJava)
    this.updates.addAll(toChange.toList.asJava)
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    val impl = this.impl.asInstanceOf[DefaultChangeDateFormatImpl]
    val schema = dataset.schema

    val columns = schema.toList
    // since this preparator only works on a single column we don't want to be selected for multiple columns
    if (columns.length != 1) {
      return 0.0f
    }

    val column = columns.head
    // since this preparator only handles string columns we don't want to be selected for other columns
    if (column.dataType != StringType) {
      return 0.0f
    }

    val maxCount = dataset.count
    // if the dataset is empty we don't want to work on it
    if (maxCount == 0L) {
      return 0.0f
    }

    val dates = dataset.rdd.map(row => row.getString(row.fieldIndex(column.name)))
    // get the strict regex by taking the one with the most matches
    // for the fuzzy regexes we try every regex since they only validate the predicted score
    val strictRegex = impl.getMostMatchedRegex(dates, fuzzy = false)

    val scores = dates
      .mapPartitions({ partition =>
        // load the scorer for each partition (to avoid broadcasting)
        val scorer = new DateFormatScorer()
        partition.map(impl.scoreDate(_, scorer, strictRegex))
      }, true)
      .sum()

    // average the resulting scores
    scores.toFloat / maxCount.toFloat
  }
}

object ChangeDateFormat {
  def apply(
             propertyName: String,
             sourceDatePattern: DatePatternEnum,
             targetDatePattern: DatePatternEnum
           ): ChangeDateFormat = {
    new ChangeDateFormat(propertyName, Option(sourceDatePattern), Option(targetDatePattern))
  }

  def apply(propertyName: String, targetDatePattern: DatePatternEnum): ChangeDateFormat = {
    new ChangeDateFormat(propertyName, targetDatePattern = Option(targetDatePattern))
  }
}
