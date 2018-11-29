package de.hpi.isg.dataprep.preparators.define

import java.nio.charset.Charset
import java.util

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.{EncodingNotSupportedException, ParameterNotSpecifiedException}
import de.hpi.isg.dataprep.metadata.{FileEncoding, PropertyDataType}
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.preparators.implementation.DefaultRemoveCharactersImpl
import de.hpi.isg.dataprep.util.{ChangeEncodingMode, DataType}

/**
  *
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class ChangeEncoding(val propertyName: String,
                     val mode: ChangeEncodingMode,
                     val userSpecifiedSourceEncoding: Option[String],
                     val userSpecifiedGoalEncoding: String) extends Preparator {

  def this(propertyName: String, mode: ChangeEncodingMode, userSpecifiedGoalEncoding: String, userSpecifiedSourceEncoding: String) {
    this(propertyName, mode, userSpecifiedGoalEncoding, userSpecifiedSourceEncoding)
  }

  this.impl = new DefaultRemoveCharactersImpl

  /**
    * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new util.ArrayList[Metadata]
    val toChange = new util.ArrayList[Metadata]

    if (!Charset.isSupported(userSpecifiedGoalEncoding)) {
      throw new EncodingNotSupportedException("Your entered goalEncoding is not supported by this preperator.")
    }
    if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("ColumnMetadata name not specified."))
    if (mode == null) throw new ParameterNotSpecifiedException(String.format("ChangeEncoding mode not specified."))
    if (userSpecifiedGoalEncoding == null) throw new ParameterNotSpecifiedException(String.format("You have at least to specify a goalEncoding"))
    if (mode eq ChangeEncodingMode.SOURCEANDAIMED) if (userSpecifiedSourceEncoding == null) throw new ParameterNotSpecifiedException(String.format("While using SOURCEANDAIMED Mode you have to specify a source encoding."))


    prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
    //prerequisites.add(new PropertyDataType(mode, ChangeEncodingMode))
    //prerequisites.add(new PropertyDataType(userSpecifiedSourceEncoding, DataType.PropertyType.STRING))
    prerequisites.add(new PropertyDataType(userSpecifiedGoalEncoding, DataType.PropertyType.STRING))

    toChange.add(new FileEncoding(propertyName, Charset.forName(userSpecifiedGoalEncoding)))

    this.prerequisites.addAll(prerequisites)
    this.updates.addAll(toChange)
  }
}
