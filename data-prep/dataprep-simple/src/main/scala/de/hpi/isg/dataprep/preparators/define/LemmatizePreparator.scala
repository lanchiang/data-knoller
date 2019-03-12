package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.LanguageMetadata.LanguageEnum
import de.hpi.isg.dataprep.metadata.{LanguageMetadata, PropertyDataType}
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DataType.PropertyType
import org.apache.spark.sql.{Dataset, Row}

class LemmatizePreparator extends AbstractPreparator with Serializable {

  var propertyName : String = _

  def this(propertyName: String) {
    this()
    this.propertyName = propertyName
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
    this.prerequisites.add(new LanguageMetadata(propertyName, null)) // any language
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

        import scala.collection.JavaConverters._
        val languageMetadata = new LanguageMetadata(propertyName, null) // any language
        val hasLanguageMetadata = targetMetadata.asScala.exists(md =>
          md.isInstanceOf[LanguageMetadata] && md.asInstanceOf[LanguageMetadata].equalsByValue(languageMetadata))

        val hasStringMetadata = DataType.getSparkTypeFromInnerType(
          dataprep.util.DataType.PropertyType.STRING).equals(schema.fields(0).dataType)

        if (hasLanguageMetadata && hasStringMetadata) {
          1
        } else {
          0
        }
      }
    }
    score
  }
}