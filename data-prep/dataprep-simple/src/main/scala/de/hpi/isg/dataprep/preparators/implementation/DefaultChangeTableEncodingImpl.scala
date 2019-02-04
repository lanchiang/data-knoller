package de.hpi.isg.dataprep.preparators.implementation

import java.nio.file.{Files, Paths}

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.exceptions.EncodingNotDetectedException
import de.hpi.isg.dataprep.load.FlatFileDataLoader
import de.hpi.isg.dataprep.metadata.CSVSourcePath
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import de.hpi.isg.dataprep.preparators.define.ChangeTableEncoding
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator
import org.mozilla.universalchardet.UniversalDetector

/**
  *
  * @author Lukas Behrendt, Lisa Ihde, Oliver Clasen
  * @since 2018/11/29
  */
class DefaultChangeTableEncodingImpl extends AbstractPreparatorImpl {
  override protected def executeLogic(abstractPreparator: AbstractPreparator,
                                      dataFrame: Dataset[Row],
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val preparator = abstractPreparator.asInstanceOf[ChangeTableEncoding]
    val pipeline = preparator.getPreparation.getPipeline

    val actualEncoding = detectEncoding(pipeline)
    val dialect = pipeline.getDialect
    dialect.setEncoding(actualEncoding)

    val dataLoader = new FlatFileDataLoader(dialect)
    val createdDataset = dataLoader.load().getDataFrame

    pipeline.initMetadataRepository()
    new ExecutionContext(createdDataset, errorAccumulator)
  }

  private def detectEncoding(pipeline: AbstractPipeline): String = {
    val csvPathMeta = new CSVSourcePath("")
    val csvPath = pipeline.getMetadataRepository.getMetadata(csvPathMeta).asInstanceOf[CSVSourcePath].getPath
    val inStream = Files.newInputStream(Paths.get(csvPath))

    val buf = new Array[Byte](4096)
    val detector = new UniversalDetector(null)

    var bytesRead = inStream.read(buf)
    while (bytesRead > 0 && !detector.isDone) {
      detector.handleData(buf, 0, bytesRead)
      bytesRead = inStream.read(buf)
    }
    detector.dataEnd()

    val encoding = detector.getDetectedCharset match {
      case null => throw new EncodingNotDetectedException(csvPath)
      case encoding => encoding
    }
    encoding
  }
}
