package de.hpi.isg.dataprep.preparators.define

import java.nio.charset.Charset
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

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {
        val prerequisites = new util.ArrayList[Metadata]
        val toChange = new util.ArrayList[Metadata]

        if (propertyName == null) throw new ParameterNotSpecifiedException("ColumnMetadata name not specified.")
        if (mode == null) throw new ParameterNotSpecifiedException("ChangeEncoding mode not specified.")
        if (userSpecifiedTargetEncoding == null) throw new ParameterNotSpecifiedException("You have at least to specify a targetEncoding")

        if (!Charset.isSupported(userSpecifiedTargetEncoding)) {
            throw new EncodingNotSupportedException("Your entered targetEncoding is not a valid Charset identifier.")
        }

        if (mode == ChangeEncodingMode.SOURCEANDTARGET) {
            if (userSpecifiedSourceEncoding == null) {
                throw new ParameterNotSpecifiedException("While using SOURCEANDTARGET Mode you have to specify a source encoding.")
            } else if (!Charset.isSupported(userSpecifiedSourceEncoding)) {
                throw new EncodingNotSupportedException("Your entered sourceEncoding is not a valid Charset identifier.")
            }
        }

        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

        toChange.add(new FileEncoding(propertyName, Charset.forName(userSpecifiedTargetEncoding)))

        this.prerequisites.addAll(prerequisites)
        //TODO: handle error if failing in preparator
        this.updates.addAll(toChange)
    }
}
