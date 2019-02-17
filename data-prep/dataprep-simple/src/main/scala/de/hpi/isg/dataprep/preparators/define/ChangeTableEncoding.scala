package de.hpi.isg.dataprep.preparators.define

import java.io.File
import java.nio.file.{Files, Paths}
import java.util

import de.hpi.isg.dataprep.metadata.{CSVSourcePath, UsedEncoding}
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
  val APPLICABILITY_THRESHOLD = 0.001
  val REPLACEMENT_CHAR = '�'

  override def buildMetadataSetup(): Unit = {
    // TODO path metadata required
  }

  def countReplacementChars(csvPath: String): Int = {
    val encoding = getCurrentEncoding.getOrElse(System.getProperty("file.encoding"))
    val replacementChar = REPLACEMENT_CHAR.toString
    val replacementBytes = replacementChar.getBytes(encoding)
    if (!replacementChar.equals(new String(replacementBytes, encoding))) {
      return 0  // current encoding cannot encode replacementChar => every replacementChar represents an error
    }

    var errorCount = 0
    var bytesRead = new Array[Byte](replacementBytes.length)
    val buf = new Array[Byte](1)
    val inStream = Files.newInputStream(Paths.get(csvPath))

    // read the csv byte by byte and search for the byte combination of the replacement char
    var hasNext = inStream.read(buf)
    while (hasNext > 0) {
      bytesRead = bytesRead.drop(1) :+ buf(0)  // shift array left and append new char
      if (bytesRead.sameElements(replacementBytes)) {
        errorCount += 1
      }
      hasNext = inStream.read(buf)
    }
    errorCount
  }

  def getCsvPath: Option[String] = {
    val dummyMetadata = new CSVSourcePath("")
    val metadata = this.getPreparation.getPipeline.getMetadataRepository.getMetadata(dummyMetadata).asInstanceOf[CSVSourcePath]
    Option(metadata.getPath)
  }

  def getCurrentEncoding: Option[String] = {
    val dummyMetadata = new UsedEncoding("")
    val metadata = this.getPreparation.getPipeline.getMetadataRepository.getMetadata(dummyMetadata).asInstanceOf[UsedEncoding]
    Option(metadata.getUsedEncoding)
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    val csvPath = this.getCsvPath.getOrElse(return 0)
    val csvFile = new File(csvPath)

    // Right now we can only handle the complete table. If we don't get it, return 0
    if (dataset != this.getPreparation.getPipeline.getRawData) {
      return 0
    }

    // count replacement chars in every row
    val errorCounter = SparkContext.getOrCreate.longAccumulator
    val replacementChar = REPLACEMENT_CHAR
    dataset.foreach(row => {
      errorCounter.add(row.toString().count(_ == replacementChar))
    })
    // make sure the replacement chars were added through decoding and were not already written in the csv
    val errors = errorCounter.value - countReplacementChars(csvPath)
    if (errors.toFloat / csvFile.length() > 0.01) 1.0f else 0.0f
  }
}