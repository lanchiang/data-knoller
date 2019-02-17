package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{DINPhoneNumberFormat, NANPPhoneNumberFormat, PhoneNumberFormat, PropertyDataType}
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.preparators.implementation.{DefaultChangePhoneFormatImpl, TaggerInstances}
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row}

import collection.JavaConverters._

class ChangePhoneFormat[A](
  val propertyName: String,
  val sourceFormat: PhoneNumberFormat[A],
  val targetFormat: PhoneNumberFormat[A]
) extends AbstractPreparator {

  def this(propertyName: String, targetFormat: PhoneNumberFormat[A]) {
    this(propertyName, null, targetFormat)
  }

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    if (propertyName == null) throw new ParameterNotSpecifiedException("Property name must be specified.")
    if (targetFormat == null) throw new ParameterNotSpecifiedException("Target format must be specified.")

    this.prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
  }

  /**
    *
    * @param schemaMapping is the schema of the input data
    * @param dataset is the input dataset
    * @param targetMetadata is the set of {@link Metadata} that shall be fulfilled for the output data
    * @return the applicability matrix succinctly represented by a hash map. Each key stands for
    */
  override def calApplicability(
    schemaMapping: SchemaMapping,
    dataset: Dataset[Row],
    targetMetadata: util.Collection[Metadata]
  ): Float = {
    import dataset.sparkSession.implicits._
    import TaggerInstances._

    val impl = this.impl.asInstanceOf[DefaultChangePhoneFormatImpl]

    targetMetadata.asScala.toSet
      .foldLeft[Option[PhoneNumberFormat[A]]](None) {
        case (_, metadata: PhoneNumberFormat[A]) => Some(metadata)
        case (optTargetFormat, _) => optTargetFormat
      }.fold(0.0f) {
        case format: PhoneNumberFormat[NANPPhoneNumberFormat] =>
          dataset.schema.toList.map { column =>
            column.dataType match {
              case StringType =>
                dataset
                  .filter(dataset(column.name).isNotNull)
                  .map(_.getAs[String](column.name))
                  .flatMap(number => impl.convert[NANPPhoneNumberFormat](number, format)(nanpTagger()).toOption.filterNot(_ == number))
                  .count().toFloat / dataset.count().toFloat
              case _ => 0f
            }
          }.max
        case format: PhoneNumberFormat[DINPhoneNumberFormat] =>
          dataset.schema.toList.map { column =>
            column.dataType match {
              case StringType =>
                dataset
                  .filter(dataset(column.name).isNotNull)
                  .map(_.getAs[String](column.name))
                  .flatMap(number => impl.convert[DINPhoneNumberFormat](number, format)(dinTagger()).toOption.filterNot(_ == number))
                  .count().toFloat / dataset.count().toFloat
              case _ => 0f
            }
          }.max
      }
  }
}
