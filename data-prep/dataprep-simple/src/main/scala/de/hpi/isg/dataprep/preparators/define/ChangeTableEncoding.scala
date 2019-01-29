package de.hpi.isg.dataprep.preparators.define

import java.io.File
import java.nio.file.{Files, Paths}
import java.util

import de.hpi.isg.dataprep.metadata.CSVSourcePath
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import org.apache.directory.api.util.Unicode
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class ChangeTableEncoding() extends AbstractPreparator {

  override def buildMetadataSetup(): Unit = {
    // TODO path metadata required
  }

  private def countErrorsInFile(csvPath: String): Int = {
    val replacementChar = new Array[Byte](3)
    replacementChar(0) = 0xEF.toByte
    replacementChar(1) = 0xBF.toByte
    replacementChar(2) = 0xBD.toByte

    var errorCount = 0

    val bytesRead = new Array[Byte](3)
    bytesRead(0) = 0x00
    bytesRead(1) = 0x00
    bytesRead(2) = 0x00

    val buf = new Array[Byte](1)
    val inStream = Files.newInputStream(Paths.get(csvPath))

    // read the csv byte by byte and search for the byte combination of the replacement char
    var byteRead = inStream.read(buf).toByte
    while (byteRead > 0) {
      bytesRead(0) = bytesRead(1)
      bytesRead(1) = bytesRead(2)
      bytesRead(2) = byteRead
      if (bytesRead.equals(replacementChar)) {
        errorCount += 1
      }
      byteRead = inStream.read(buf).toByte
    }
    errorCount
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    val replacementChar = new Array[Byte](3)
    replacementChar(0) = 0xEF.toByte
    replacementChar(1) = 0xBF.toByte
    replacementChar(2) = 0xBD.toByte

    val dummyMetadata = new CSVSourcePath("")
    val csvMetadata = this.getPreparation.getPipeline.getMetadataRepository.getMetadata(dummyMetadata).asInstanceOf[CSVSourcePath]
    if (csvMetadata == null) {
      return 0
    }
    val csvFile = new File(csvMetadata.getPath)

    // Right now we can only handle the complete table. If we don't get it, return 0
    if (dataset != this.getPreparation.getPipeline.getRawData) {
      return 0
    }

    // count replacement chars in every row
    val errorCounter = SparkContext.getOrCreate.longAccumulator
    dataset.foreach(row => {
      errorCounter.add(row.toString().count(_ == Unicode.bytesToChar(replacementChar)))
    })
    // make sure the replacement chars were added through decoding and were not already written in the csv
    dataset.show()
    val errors = errorCounter.value - countErrorsInFile(csvMetadata.getPath)
    errors.toFloat / csvFile.length()
  }
}