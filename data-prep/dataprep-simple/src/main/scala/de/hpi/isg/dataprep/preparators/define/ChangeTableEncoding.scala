package de.hpi.isg.dataprep.preparators.define

import java.io.File
import java.nio.file.{Files, Paths}
import java.util

import de.hpi.isg.dataprep.metadata.CSVSourcePath
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import org.apache.directory.api.util.Unicode
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class ChangeTableEncoding(val propertyName: String,
                          var userSpecifiedSourceEncoding: String,
                          val userSpecifiedTargetEncoding: String) extends AbstractPreparator {

  def this(propertyName: String, userSpecifiedTargetEncoding: String) {
    this(propertyName, null, userSpecifiedTargetEncoding)
  }

  override def buildMetadataSetup(): Unit = {
    // TODO
  }

  private def countErrorsInFile(): Int = {
    val replacementChar = new Array[Byte](3)
    replacementChar(0) = Byte(0xEF)
    replacementChar(1) = Byte(0xBF)
    replacementChar(0) = Byte(0xBD)

    var errorCount = 0

    val bytesRead = new Array[Byte](3)
    bytesRead(0) = 0x00
    bytesRead(1) = 0x00
    bytesRead(2) = 0x00

    val buf = new Array[Byte](1)
    val inStream = Files.newInputStream(Paths.get(this.getPreparation.getPipeline.getMetadataRepository.getMetadata(CSVSourcePath).toString))

    var byteRead = inStream.read(buf)
    while (byteRead > 0) {
      bytesRead(0) = bytesRead(1)
      bytesRead(1) = bytesRead(2)
      bytesRead(2) = Byte(byteRead)
      if (bytesRead.equals(replacementChar)) {
        errorCount += 1
      }
      byteRead = inStream.read(buf)
    }
    errorCount
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    val replacementChar = new Array[Byte](3)
    replacementChar(0) = Byte(0xEF)
    replacementChar(1) = Byte(0xBF)
    replacementChar(0) = Byte(0xBD)
    var errorCounter = 0

    val csvFile = new File(this.getPreparation.getPipeline.getMetadataRepository.getMetadata(CSVSourcePath).toString)

    if (dataset != this.getPreparation.getPipeline.getRawData) {
      return 0
    }

    dataset.foreach(row => {
      errorCounter = errorCounter + row.toString().count(p => p == Unicode.bytesToChar(replacementChar))
    })
    errorCounter = errorCounter - countErrorsInFile()
    errorCounter / csvFile.length()
  }
}
