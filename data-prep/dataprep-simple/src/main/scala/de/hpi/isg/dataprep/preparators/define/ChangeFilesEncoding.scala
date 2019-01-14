package de.hpi.isg.dataprep.preparators.define

import java.io.File
import java.nio.charset.{Charset, IllegalCharsetNameException}
import java.util

import de.hpi.isg.dataprep.exceptions.{EncodingNotSupportedException, ParameterNotSpecifiedException}
import de.hpi.isg.dataprep.metadata.{FileEncoding, PropertyDataType}
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

import scala.io.{Codec, Source}

/**
  *
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class ChangeFilesEncoding(val propertyName: String,
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

  private def readFile(file: String, inputEncoding: Charset): String = Source.fromFile(new File(file))(new Codec(inputEncoding)).mkString


  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]):
  Float = {
    //TODO: how do I get the fileEncoding? this way i only get a "metadata" object but can't access the fileEncoding :(
    val sourceEncoding = this.getPreparation.getPipeline.getMetadataRepository.getMetadata(FileEncoding)
    val targetEncoding = targetMetadata.stream().filter()
    if (sourceEncoding.equals(targetEncoding))
      0
    else {
      val errorCounter = 0
      val rows = this.getPreparation.getPipeline.getRawData
      rows.foreach(
        row => {
          try ({
            //TODO: how can I readFile for each column in one row? Was not able to get the columns and go for each column like it was possible in DDM
            val content = readFile(row.toString(), sourceEncoding)
            if (content == null) errorCounter + 1
          })
        }
      )
      if ((errorCounter / rows.count()) > 0.2) 1
      else 0
    }
  }

}
