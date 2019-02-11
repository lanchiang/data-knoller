package de.hpi.isg.dataprep.preparators.implementation

import java.nio.file.{Files, Paths}

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.exceptions.EncodingNotDetectedException
import de.hpi.isg.dataprep.load.FlatFileDataLoader
import de.hpi.isg.dataprep.metadata.CSVSourcePath
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import de.hpi.isg.dataprep.preparators.define.ChangeTableEncoding
import org.apache.spark.sql.{DataFrame, Dataset, Row}
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
    var data = dataFrame
    val dialect = pipeline.getDialect

    val loadEncoding = preparator.getCurrentEncoding
    val actualEncoding = detectEncoding(pipeline)
    val hasCorrectLoadEncoding = loadEncoding.isDefined && loadEncoding.get == actualEncoding

    if (!hasCorrectLoadEncoding) {
      dialect.setEncoding(actualEncoding)
      data = reloadWith(dialect, pipeline)
    }

    if (hasCorrectLoadEncoding || preparator.calApplicability(null, data, null) > 0) {
      val unmixedDialect = unmixEncoding()
      data = reloadWith(unmixedDialect, pipeline)
    }
    new ExecutionContext(data, errorAccumulator)
  }

  private def reloadWith(dialect: FileLoadDialect, pipeline: AbstractPipeline): DataFrame = {
    val createdDataset = new FlatFileDataLoader(dialect).load().getDataFrame
    pipeline.initMetadataRepository()
    createdDataset
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

    val encoding = detector.getDetectedCharset
    if (encoding == null) throw new EncodingNotDetectedException(csvPath)
    encoding
  }

  private def unmixEncoding(): FileLoadDialect = {
    new FileLoadDialect
  }
}
