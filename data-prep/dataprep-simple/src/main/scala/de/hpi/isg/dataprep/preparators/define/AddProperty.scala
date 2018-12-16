package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.preparators.implementation.DefaultAddPropertyImpl
import de.hpi.isg.dataprep.util.DataType.PropertyType

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

  /**
    * Calculate the matrix of preparator applicability to the data. In the matrix, each
    * row represent a specific signature of the preparator, while each column represent a specific
    * {@link ColumnCombination} of the data
    *
    * @return the applicability matrix succinctly represented by a hash map. Each key stands for
    *         a { @link ColumnCombination} in the dataset, and its value the applicability score of this preparator signature.
    */
  override def calApplicability(): util.Map[ColumnCombination, lang.Float] = {
    null
  }
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
