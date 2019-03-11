package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.DateScorer
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeDateFormatImpl
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
    * Empty parameter constructor for the decision engine.
    */
  def this() { this(null) }

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

  /**
    * The applicability score for this preparator is calculated the following way:
    * Each date is classified as either a date (1.0) or no date (0.0). These scores are then averaged over the whole
    * dataset. The binary classification is a combination of regex matching and using a keras deep learning model.
    *
    * @param schemaMapping is the schema of the input data towards the schema of the output data.
    * @param dataset is the input dataset slice. A slice can be a subset of the columns of the data,
    *                or a subset of the rows of the data.
    * @param targetMetadata is the set of {@link Metadata} that shall be fulfilled for the output data
    *     */
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

    // true if a property date pattern metadata field for the current column exists in the target metadata
    val alreadyExecuted = targetMetadata.asScala.exists {
      case metadata: PropertyDatePattern =>
        // check of the column in the metadata equals the current dataset column
        metadata.getScope.getName == column.name
      case _ => false
    }
    // if the preparator was already executed on this column then we don't want to be executed again
    if (alreadyExecuted) {
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

    // since the model is small we can load it once and broadcast it
    val scorer = new DateScorer()

    // score the dates and calculate the final score as the average of the individual scores
    val scores = dates
      .map(impl.scoreDate(_, scorer, strictRegex))
      .sum()
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
