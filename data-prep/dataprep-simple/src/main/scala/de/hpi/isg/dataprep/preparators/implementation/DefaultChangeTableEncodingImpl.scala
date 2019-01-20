package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.{DialectBuilder, ExecutionContext}
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.load.FlatFileDataLoader
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
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
    val actualEncoding = detectEncoding(dataFrame)

    val pipeline = preparator.getPreparation.getPipeline
    val dialect = pipeline.getDialect
    dialect.setEncoding(actualEncoding)

    val dataLoader = new FlatFileDataLoader(dialect)
    val createdDataset = dataLoader.load().getDataFrame

    pipeline.initMetadataRepository()
    new ExecutionContext(createdDataset, errorAccumulator)
  }

  private def detectEncoding(value: Dataset[Row]): String = {
    // TODO
    "UTF-8"
  }
}
