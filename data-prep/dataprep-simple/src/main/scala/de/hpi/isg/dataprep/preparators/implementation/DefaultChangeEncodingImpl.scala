package de.hpi.isg.dataprep.preparators.implementation

import java.io.{File, IOException}
import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangeEncoding
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.io.{Codec, Source}
import scala.util.{Failure, Success, Try}
import org.mozilla.universalchardet.UniversalDetector;


/**
  *
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class DefaultChangeEncodingImpl extends PreparatorImpl {
    override protected def executeLogic(abstractPreparator: AbstractPreparator,
                                        dataFrame: Dataset[Row],
                                        errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
        val preparator = abstractPreparator.asInstanceOf[ChangeEncoding]
        val propertyName = preparator.propertyName
        val convertEncodingFunc = ConversionHelper2.convertEncodingFunc(preparator.userSpecifiedSourceEncoding,
                                                                        preparator.userSpecifiedTargetEncoding,
                                                                        errorAccumulator)

        val createdDataset = dataFrame.withColumn(propertyName, convertEncodingFunc(col(propertyName)))
        // since Spark is lazy, we force it to execute the transformation immediately in order to populate errorAccumulator
        createdDataset.first()

        new ExecutionContext(createdDataset, errorAccumulator)
    }
}

object ConversionHelper2 { // TODO prettify, rename, document
    def convertEncodingFunc(inputEncoding: String,
                            outputEncoding: String,
                            errorAccumulator: CollectionAccumulator[PreparationError]): UserDefinedFunction =
        udf(convertEncoding(inputEncoding, outputEncoding, errorAccumulator) _)

    def readFile(file: String, inputEncoding: Charset): String = Source.fromFile(new File(file))(new Codec(inputEncoding)).mkString

    def writeFile(outputPath: Path, content: String, outputEncoding: Charset): Unit = {
        Files.write(
            outputPath,
            content.getBytes(outputEncoding),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
        )
    }

    def convertEncoding(inputEncodingOrNull: String,
                        outputEncoding: String,
                        errorAccumulator: CollectionAccumulator[PreparationError])(fileName: String): String = {
        val inputEncoding = if (inputEncodingOrNull != null) inputEncodingOrNull else inferInputEncoding(fileName)
        val newFileName = fileName + ".new"  // TODO
        Try({
            val content = readFile(fileName, Charset.forName(inputEncoding))
            writeFile(Paths.get(newFileName), content, Charset.forName(outputEncoding))
        }) match {
            case Success(_) => newFileName
            case Failure(ex) =>
                errorAccumulator.add(new RecordError(fileName, ex))
                fileName
        }
    }

    def inferInputEncoding(fileName: String): String = {
        val buf = new Array[Byte](4096)
        val inStream = Files.newInputStream(Paths.get(fileName))

        val detector = new UniversalDetector(null)

        var bytesRead = inStream.read(buf)
        while (bytesRead > 0 && !detector.isDone) {
            detector.handleData(buf, 0, bytesRead)
            bytesRead = inStream.read(buf)
        }
        detector.dataEnd()

        detector.getDetectedCharset  // TODO handle null
    }
}
