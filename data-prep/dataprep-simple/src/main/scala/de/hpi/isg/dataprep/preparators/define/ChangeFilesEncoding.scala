package de.hpi.isg.dataprep.preparators.define

import java.nio.charset.{Charset, IllegalCharsetNameException}
import java.nio.file.{Files, Paths}
import java.util

import de.hpi.isg.dataprep.exceptions.{EncodingNotSupportedException, ParameterNotSpecifiedException}
import de.hpi.isg.dataprep.metadata.{FileEncoding, PropertyDataType}
import de.hpi.isg.dataprep.model.repository.MetadataRepository
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

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

  def this() {
    this(null, null)
  }

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

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata], pipeline: AbstractPipeline):
  Float = {
    if (dataset.columns.length != 1) return 0
    val propertyName = dataset.columns(0)

    val targetEncodings = targetMetadata.toArray(Array[Metadata]()).flatMap({
      case encoding: FileEncoding => Some(encoding)
      case _ => None
    })

    val targetEncoding = targetEncodings.find(e => e.getPropertyName.equals(propertyName)).getOrElse(return 0)

    val sourceEncoding = this.getPreparation.getPipeline.getMetadataRepository.getMetadata(targetEncoding).asInstanceOf[FileEncoding]
    if (sourceEncoding != null && targetEncoding.getFileEncoding.equals(sourceEncoding.getFileEncoding)) return 0

    var errorCounter = 0
    dataset.foreach(row => {
      val fileName = row.getString(0)
      if (!Files.exists(Paths.get(fileName))) errorCounter += 1
    })

    val rows = dataset.count()
    1 - (errorCounter / rows)
  }
}
