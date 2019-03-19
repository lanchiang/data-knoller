package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.{Attribute, Schema, SchemaMapping}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultDeletePropertyImpl
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.ListBuffer

/**
  * Delete a property in a dataset.
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class DeleteProperty extends AbstractPreparator {

  var propertyName : String = _

  def this(propertyName : String) {
    this()
    this.propertyName = propertyName
  }

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new util.ArrayList[Metadata]
    val toChange = new util.ArrayList[Metadata]

    if (propertyName == null) {
      throw new ParameterNotSpecifiedException("Property name not specified!")
    }

    this.prerequisites.addAll(prerequisites)
    this.updates.addAll(toChange)
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    val schema = dataset.schema
    val score : Float = schema.size match {
      case value if value > 1 => 0
      case value if value == 0 => {
        val e : Exception = new RuntimeException(new IllegalArgumentException("Illegal schema size."))
        e.printStackTrace()
        0
      }
      case 1 => {
        this.propertyName = schema.fields(0).name
        val targetAttributes : util.Set[Attribute] = schemaMapping.getTargetBySourceAttributeName(schema.fields(0).name)
        val score = targetAttributes.size().toFloat match {
          case 0 => {
            // this attribute corresponds to nothing in the target schema, meaning that it is deleted at some point, therefore
            // return 1 as the score
            1
          }
          case length if length > 0 => {
            // if this attribute corresponds to any other attributes in the target schema, it should not be deleted now.
            // the current implementation give lower score to the attribute that corresponds to more attributes in the target schema.
            1/(length+1)
          }
        }
        score
      }
    }
    score
  }
}
