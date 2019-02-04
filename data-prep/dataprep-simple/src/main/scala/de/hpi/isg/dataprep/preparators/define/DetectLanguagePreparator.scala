package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.LanguageMetadata.LanguageEnum
import de.hpi.isg.dataprep.metadata.{LanguageMetadata, PropertyDataType}
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Attribute, SchemaMapping}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}
import scala.collection.JavaConverters._
import org.dmg.pmml.False

class DetectLanguagePreparator() extends AbstractPreparator {

  var propertyName : String = _
  var chunkSize : Int = 5

  def this(propertyName: String) {
    this()
    this.propertyName = propertyName
  }

  def this(propertyName: String, chunkSize: Int) {
    this()
    this.propertyName = propertyName
    this.chunkSize = chunkSize
  }

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {
    if (propertyName == null)
      throw new ParameterNotSpecifiedException(String.format("ColumnMetadata name not specified."))
    this.prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    val schema = dataset.schema
    val score : Float = schema.size match {
      case value if value > 1 => 0
      case value if value == 0 => {
        //        throw new RuntimeException("Illegal schema size.")
        val e : Exception = new RuntimeException(new IllegalArgumentException("Illegal schema size."))
        e.printStackTrace()
        0
      }
      case 1 => {
        propertyName = schema.fields(0).name
        val metadataRepository = this.getPreparation().getPipeline().getMetadataRepository()

        val languageMetadata = new LanguageMetadata(propertyName, null) // any language
        val checkLanguageMetadata = metadataRepository.getMetadata(languageMetadata)
        val stringMetadata = new PropertyDataType(propertyName, DataType.PropertyType.STRING)
        val hasStringMetadata = metadataRepository.containByValue(stringMetadata)

        if ((checkLanguageMetadata == null || !languageMetadata.equalsByValue(checkLanguageMetadata)) && hasStringMetadata) {
          val newSample = dataset
            .sample(withReplacement = true, math.min(1.0, 20.0 / dataset.count()))
            .limit(10)

          import dataset.sparkSession.implicits._
          val sampleString = newSample.select(propertyName).map(_.getAs[String](0)).collect().mkString("")
          val cleanedStr = sampleString.replaceAll("[\\d.,\\/#!$%\\^&\\*;:{}=\\-_`~()]", "")

          val score = math.max(0.001, 1 - (5 / cleanedStr.length)).asInstanceOf[Float]
          score
        } else {
          0
        }
      }
    }
    score
  }
}