package de.hpi.isg.dataprep.preparators.define

import java.nio.charset.{Charset, IllegalCharsetNameException}

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.{EncodingNotSupportedException, ParameterNotSpecifiedException}
import de.hpi.isg.dataprep.metadata.{FileEncoding, PropertyDataType}
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeEncodingImpl
import de.hpi.isg.dataprep.util.DataType

/**
  *
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class ChangeEncoding(val propertyName: String,
                     val userSpecifiedSourceEncoding: String,
                     val userSpecifiedTargetEncoding: String) extends Preparator {

    def this(propertyName: String, userSpecifiedTargetEncoding: String) {
        this(propertyName, null, userSpecifiedTargetEncoding)
    }

    this.impl = new DefaultChangeEncodingImpl

    override def buildMetadataSetup(): Unit = {
        if (propertyName == null) throw new ParameterNotSpecifiedException("Column name not specified.")
        if (userSpecifiedTargetEncoding == null) throw new ParameterNotSpecifiedException("You have to specify at least a target encoding.")
        verifyEncoding(userSpecifiedTargetEncoding, target = true)

        if (userSpecifiedSourceEncoding != null) {
            verifyEncoding(userSpecifiedSourceEncoding, target = false)
        }

        this.prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
        this.updates.add(new FileEncoding(propertyName, Charset.forName(userSpecifiedTargetEncoding)))
    }

    private def verifyEncoding(encoding: String, target: Boolean): Unit = {
        if (!Charset.isSupported(encoding)) throw new EncodingNotSupportedException(s"$encoding is not supported by your JVM.")
        if (target && !Charset.forName(encoding).canEncode) throw new IllegalCharsetNameException(encoding)
    }
}
