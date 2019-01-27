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

  /**
    * Calculate the matrix of preparator applicability to the data. In the matrix, each row represent a specific signature of
    * the preparator, while each column represent a specific column combinationof the data
    *
    * @return the applicability matrix succinctly represented by a hash map. Each key stands for a column combination in the dataset,
    *         and its value the applicability score of this preparator signature.
    */
  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    val schema = dataset.schema
    val score : Float = schema.size match {
      case value if value > 1 => 0
      case value if value == 0 => throw new RuntimeException("Illegal schema size.")
      case 1 => {
        val targetAttributes : util.Set[Attribute] = schemaMapping.getTargetBySourceAttributeName(dataset.schema.fields(0).name)
        val score = targetAttributes.size() match {
          case 0 => {
            // this attribute corresponds to nothing in the target schema, meaning that it is deleted at some point, therefore
            // return 1 as the score
            1
          }
          case length if length > 0 => {
            // if this attribute corresponds to any other attributes in the target schema, it should not be deleted now.
            // the current implementation give lower score to the more such attributes it corresponds to.
            1/length
          }
        }
        score
      }
    }
    score
  }
}
