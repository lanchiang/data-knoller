package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PhoneNumberFormat, PhoneNumberFormatComponentType, PropertyDataType}
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangePhoneFormatImpl
import de.hpi.isg.dataprep.preparators.implementation.{CheckerInstances, CheckerSyntax, TaggerInstances}
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row}

import collection.JavaConverters._

class ChangePhoneFormat(
  val propertyName: String,
  val sourceFormat: PhoneNumberFormat,
  val targetFormat: PhoneNumberFormat
) extends AbstractPreparator {

  def this(propertyName: String, targetFormat: PhoneNumberFormat) {
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
    import CheckerInstances._
    import CheckerSyntax._
    import TaggerInstances._

    val impl = this.impl.asInstanceOf[DefaultChangePhoneFormatImpl]
    val tagger = phoneNumberTagger(PhoneNumberFormatComponentType.ordered)

    targetMetadata.asScala.toSet
      .foldLeft[Option[PhoneNumberFormat]](None) {
        case (_, metadata: PhoneNumberFormat) => Some(metadata)
        case (optTargetFormat, _) => optTargetFormat
      }.fold(0.0f) { format =>
        dataset.schema.toList.map { column =>
          column.dataType match {
            case StringType =>
              val samples = dataset.sample(false, 0.01)
              val filteredSamples = samples.filter(samples(column.name).isNotNull)

              val forallMatching = filteredSamples
                .map(_.getAs[String](column.name).matchesFormat(format))
                .reduce(_ && _)

              val convertibles = filteredSamples
                .map(_.getAs[String](column.name))
                .flatMap(number => impl.convert(number, format)(tagger).toOption)

              if (forallMatching) 0f else convertibles.count().toFloat / samples.count().toFloat
            case _ => 0f
          }
        }.max
      }
  }
}
