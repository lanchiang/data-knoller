package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.preparators.implementation.DefaultAddPropertyImpl
import de.hpi.isg.dataprep.util.DataType.PropertyType

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class AddProperty(val targetPropertyName : String,
                  val targetType: PropertyType,
                  val position : Int,
                  val filling : Any) extends Preparator {

    def this(targetPropertyName : String, targetType : PropertyType) {
        this(targetPropertyName, targetType, AddProperty.DEFAULT_POSITION, AddProperty.DEFAULT_VALUE(targetType))
    }

    def this(targetPropertyName : String, targetType : PropertyType, position : Int) {
        this(targetPropertyName, targetType, position, AddProperty.DEFAULT_VALUE(targetType))
    }

//    override def newImpl = new DefaultAddPropertyImpl

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws ParameterNotSpecifiedException
      */
    @throws(classOf[ParameterNotSpecifiedException])
    override def buildMetadataSetup(): Unit = {

    }
}

object AddProperty {

    private val DEFAULT_POSITION = 0

    val DEFAULT_VALUE = (targetType : PropertyType) => {
        targetType match {
            case PropertyType.INTEGER => 0
            case PropertyType.DOUBLE => 0.0
            case PropertyType.STRING => ""
            case PropertyType.DATE => ""
        }
    }
}
