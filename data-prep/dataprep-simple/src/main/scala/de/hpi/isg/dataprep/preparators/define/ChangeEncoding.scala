package de.hpi.isg.dataprep.preparators.define

import java.nio.charset.{Charset, IllegalCharsetNameException}
import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.{EncodingNotSupportedException, ParameterNotSpecifiedException}
import de.hpi.isg.dataprep.metadata.{FileEncoding, PropertyDataType}
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeEncodingImpl
import de.hpi.isg.dataprep.util.DataType

/**
  *
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class ChangeEncoding(val propertyName: String,
                     var userSpecifiedSourceEncoding: String,
                     val userSpecifiedTargetEncoding: String) extends AbstractPreparator {

  def this(propertyName: String, userSpecifiedTargetEncoding: String) {
    this(propertyName, null, userSpecifiedTargetEncoding)
  }

  //    override def newImpl = new DefaultChangeEncodingImpl

  override def buildMetadataSetup(): Unit = {
    if (propertyName == null) throw new ParameterNotSpecifiedException("ColumnCombination name not specified.")
    if (userSpecifiedTargetEncoding == null) throw new ParameterNotSpecifiedException("You have to specify at least a target encoding.")
    verifyEncoding(userSpecifiedTargetEncoding, target = true)

    if (userSpecifiedSourceEncoding != null) {
      verifyEncoding(userSpecifiedSourceEncoding, target = false)
    }

    this.prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
    this.updates.add(new FileEncoding(propertyName, userSpecifiedTargetEncoding))
  }

  private def verifyEncoding(encoding: String, target: Boolean): Unit = {
    if (!Charset.isSupported(encoding)) throw new EncodingNotSupportedException(s"$encoding is not supported by your JVM.")
    if (target && !Charset.forName(encoding).canEncode) throw new IllegalCharsetNameException(encoding)
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
