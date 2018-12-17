package de.hpi.isg.dataprep.preparators.define

import java.nio.charset.{Charset, IllegalCharsetNameException}
import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.{EncodingNotSupportedException, ParameterNotSpecifiedException}
import de.hpi.isg.dataprep.metadata.{FileEncoding, PropertyDataType}
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeEncodingImpl
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

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


  override def calApplicability(dataset: Dataset[Row], sourceSchema: Schema, targetSchema: Schema, targetMetadata: util.Collection[Metadata]): Float = {
    0
  }
}
