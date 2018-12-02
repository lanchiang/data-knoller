package de.hpi.isg.dataprep.preparators.define

import java.nio.charset.{Charset, IllegalCharsetNameException}
import java.util

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.{EncodingNotSupportedException, ParameterNotSpecifiedException}
import de.hpi.isg.dataprep.metadata.{FileEncoding, PropertyDataType}
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeEncodingImpl
import de.hpi.isg.dataprep.util.{ChangeEncodingMode, DataType}

/**
  *
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class ChangeEncoding(val propertyName: String,
                     val mode: ChangeEncodingMode,
                     val userSpecifiedSourceEncoding: String,
                     val userSpecifiedTargetEncoding: String) extends Preparator {

    def this(propertyName: String, mode: ChangeEncodingMode, userSpecifiedTargetEncoding: String) {
        this(propertyName, mode, null, userSpecifiedTargetEncoding)
    }

    this.impl = new DefaultChangeEncodingImpl

    override def buildMetadataSetup(): Unit = {
        this.prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
        this.updates.add(new FileEncoding(propertyName, Charset.forName(userSpecifiedTargetEncoding)))

        if (propertyName == null) throw new ParameterNotSpecifiedException("Column name not specified.")
        if (mode == null) throw new ParameterNotSpecifiedException("ChangeEncoding mode not specified.")
        if (userSpecifiedTargetEncoding == null) throw new ParameterNotSpecifiedException("You have to specify at least a target encoding.")
        verifyEncoding(userSpecifiedTargetEncoding, target = true)

        if (mode == ChangeEncodingMode.SOURCEANDTARGET) {
            if (userSpecifiedSourceEncoding == null) {
                throw new ParameterNotSpecifiedException("While using SOURCEANDTARGET mode you have to specify a source encoding.")
            }
            verifyEncoding(userSpecifiedSourceEncoding, target = false)
        }
    }

    private def verifyEncoding(encoding: String, target: Boolean): Unit = {
        if (!Charset.isSupported(encoding)) throw new EncodingNotSupportedException(s"$encoding is not supported by your JVM.")
        if (target && !Charset.forName(encoding).canEncode) throw new IllegalCharsetNameException(encoding)
    }
}
