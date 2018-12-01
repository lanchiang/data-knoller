package de.hpi.isg.dataprep.preparators.implementation

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.PreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.ChangeEncoding
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.io.{Codec, Source}


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

    val createdDataset = dataFrame.withColumn( propertyName, convertEncodingFunc(col(propertyName)))
    new ExecutionContext(createdDataset, errorAccumulator)
  }
}

object ConversionHelper2 { // TODO prettify, rename, document
    def convertEncodingFunc(inputEncoding: String,
                            outputEncoding: String,
                            errorAccumulator: CollectionAccumulator[PreparationError]): UserDefinedFunction =
        udf(convertEncoding(inputEncoding, outputEncoding, errorAccumulator) _)

    def readFile(file: File, inputEncoding: Charset): String = Source.fromFile(file)(new Codec(inputEncoding)).mkString

    def writeFile(outputPath: Path, content: String, outputEncoding: Charset): Unit = {
        Files.write(
            outputPath,
            content.getBytes(outputEncoding),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
        )
    }

    def convertEncoding(inputEncoding: String,
                        outputEncoding: String,
                        collectionAccumulator: CollectionAccumulator[PreparationError])(fileName: String): String = {
        val newFileName = fileName + ".new"  // TODO
        val content = readFile(new File(fileName), Charset.forName(inputEncoding))
        writeFile(Paths.get(newFileName), content, Charset.forName(outputEncoding))
        newFileName
    }
}
