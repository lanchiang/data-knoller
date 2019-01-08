package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.preparators.implementation.DefaultAddPropertyImpl
import de.hpi.isg.dataprep.util.DataType.PropertyType
import org.apache.spark.sql.{Dataset, Row}

import scala.runtime.Nothing$

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class AddProperty(val targetPropertyName: String,
                  val targetType: PropertyType,
                  val position: Int,
                  val filling: Any) extends AbstractPreparator {

  def this(targetPropertyName: String, targetType: PropertyType) {
    this(targetPropertyName, targetType, AddProperty.DEFAULT_POSITION, AddProperty.DEFAULT_VALUE(targetType))
  }

  def this(targetPropertyName: String, targetType: PropertyType, position: Int) {
    this(targetPropertyName, targetType, position, AddProperty.DEFAULT_VALUE(targetType))
  }

  //    override def newImpl = new DefaultAddPropertyImpl

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  @throws(classOf[ParameterNotSpecifiedException])
  override def buildMetadataSetup(): Unit = {

  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = 0
}

object AddProperty {

  private val DEFAULT_POSITION = 0

  val DEFAULT_VALUE = (targetType: PropertyType) => {
    targetType match {
      case PropertyType.INTEGER => 0
      case PropertyType.DOUBLE => 0.0
      case PropertyType.STRING => ""
      case PropertyType.DATE => ""
    }
  }
}
