package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.load.FlatFileDataLoader
import de.hpi.isg.dataprep.metadata.CSVSourcePath
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
import de.hpi.isg.dataprep.preparators.define.ChangeTableEncoding
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

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
    val bufferedSource = scala.io.Source.fromFile(csvPath)
    val encoding = bufferedSource.reader.getEncoding
    bufferedSource.close
    encoding
  }
}
