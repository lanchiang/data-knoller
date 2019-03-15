package de.hpi.isg.dataprep.preparators.define

import java.util.Optional
import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Attribute, SchemaMapping}
import de.hpi.isg.dataprep.util.DataType.PropertyType
import org.apache.spark.sql.{Dataset, Row}

import scala.util.Random

/**
  * This preparator adds a new property(column) to the dataset.
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class AddProperty extends AbstractPreparator {

  var targetPropertyName: String = _
  var targetType: PropertyType = _
  var position: Int = _
  var filling: Any = _

  def this(_targetPropertyName: String,
           _targetType: PropertyType,
           _position: Int,
           _filling: Any) {
    this()
    targetPropertyName = _targetPropertyName
    targetType = _targetType
    position = _position
    filling = _filling
  }

  def this(targetPropertyName: String, targetType: PropertyType) {
    this(targetPropertyName, targetType, AddProperty.DEFAULT_POSITION, AddProperty.DEFAULT_VALUE(targetType))

  }

  def this(targetPropertyName: String, targetType: PropertyType, position: Int) {
    this(targetPropertyName, targetType, position, AddProperty.DEFAULT_VALUE(targetType))
  }

  override def buildMetadataSetup(): Unit = {}

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
//    val score = Random.nextFloat()
//    score
    0
  }
}

object AddProperty {

  private val DEFAULT_POSITION = 0

  val DEFAULT_PROPERTY_NAME = null

  val DEFAULT_FILLING = "0"

  val DEFAULT_VALUE = (targetType: PropertyType) => {
    targetType match {
      case PropertyType.INTEGER => 0
      case PropertyType.DOUBLE => 0.0
      case PropertyType.STRING => ""
      case PropertyType.DATE => ""
        // now the default is undetermined
      case _ => ""
    }
  }
}
