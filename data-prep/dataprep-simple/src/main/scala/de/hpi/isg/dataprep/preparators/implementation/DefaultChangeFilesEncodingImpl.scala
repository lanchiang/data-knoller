package de.hpi.isg.dataprep.preparators.implementation

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.exceptions.{EncodingNotDetectedException, ImproperTargetEncodingException}
import de.hpi.isg.dataprep.metadata.FileEncoding
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.repository.MetadataRepository
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.define.ChangeFilesEncoding
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator
import org.mozilla.universalchardet.UniversalDetector

import scala.io.{Codec, Source}
import scala.util.{Failure, Success, Try}


/**
  *
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class DefaultChangeFilesEncodingImpl extends AbstractPreparatorImpl {
  override protected def executeLogic(abstractPreparator: AbstractPreparator,
                                      dataFrame: Dataset[Row],
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[ChangeFilesEncoding]
    val propertyName = preparator.propertyName
    var sourceEncoding = preparator.userSpecifiedSourceEncoding
    if (sourceEncoding == null) sourceEncoding = getEncodingFromMetadata(preparator)

    val convertEncodingFunc = EncodingConversionHelper.conversionFunc(sourceEncoding,
      preparator.userSpecifiedTargetEncoding,
      errorAccumulator)

    val createdDataset = dataFrame.withColumn(propertyName, convertEncodingFunc(col(propertyName)))
//    createdDataset.first() // Spark is lazy => we force it to execute the transformation immediately to populate errorAccumulator

    createdDataset.foreach(row => Unit)

    new ExecutionContext(createdDataset, errorAccumulator)
  }

  private def getEncodingFromMetadata(preparator: ChangeFilesEncoding): String = {
    val metadataRepository = preparator.getPreparation.getPipeline.getMetadataRepository
    val mockMetadata = new FileEncoding(preparator.propertyName, null)
    val metadataOrNull = metadataRepository.getMetadata(mockMetadata)
    metadataOrNull match {
      case metadataEncoding: FileEncoding => metadataEncoding.getFileEncoding
      case null => null
    }
  }
}

object EncodingConversionHelper {
  // udf wrapper for convertEncoding
  // the timestamp ensures that multiple executions of the udf don't create multiple files with the same content
  def conversionFunc(inputEncoding: String,
                     outputEncoding: String,
                     errorAccumulator: CollectionAccumulator[PreparationError]): UserDefinedFunction =
    udf(convertEncoding(inputEncoding, outputEncoding, errorAccumulator, System.currentTimeMillis) _)

  // reads a file, converts it to outputEncoding and saves it under a new name
  // returns the new file name if the conversion was successful, the old file name otherwise
  private def convertEncoding(inputEncodingOrNull: String,
                              outputEncoding: String,
                              errorAccumulator: CollectionAccumulator[PreparationError],
                              timestamp: Timestamp)(fileName: String): String = {
    val newFileName = generateNewFileName(fileName, timestamp)

    Try({
      val inputEncoding = if (inputEncodingOrNull == null) inferInputEncoding(fileName) else inputEncodingOrNull
      val content = readFile(fileName, Charset.forName(inputEncoding))
      writeFile(Paths.get(newFileName), content, Charset.forName(outputEncoding))
    }) match {
      case Success(_) => newFileName
      case Failure(ex) =>
        errorAccumulator.add(new RecordError(fileName, ex))
        fileName
    }
  }

  private def readFile(file: String, inputEncoding: Charset): String = Source.fromFile(new File(file))(new Codec(inputEncoding)).mkString

  private def writeFile(outputPath: Path, content: String, outputEncoding: Charset): Unit = {
    if (!outputEncoding.newEncoder().canEncode(content)) throw new ImproperTargetEncodingException(content, outputEncoding)
    Files.write(
      outputPath,
      content.getBytes(outputEncoding),
      StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
    )
  }

  private def inferInputEncoding(fileName: String): String = {
    val buf = new Array[Byte](4096)
    val inStream = Files.newInputStream(Paths.get(fileName))

    val detector = new UniversalDetector(null)

    var bytesRead = inStream.read(buf)
    while (bytesRead > 0 && !detector.isDone) {
      detector.handleData(buf, 0, bytesRead)
      bytesRead = inStream.read(buf)
    }
    detector.dataEnd()

    detector.getDetectedCharset match {
      case null => throw new EncodingNotDetectedException(fileName)
      case encoding => encoding
    }
  }

  private def generateNewFileName(fileName: String, timestamp: Timestamp): String = {
    val file = new File(fileName)
    val dirName = file.getParent
    val newFileName = timestamp.toString + "_" + file.getName
    dirName + File.separator + newFileName
  }
}
