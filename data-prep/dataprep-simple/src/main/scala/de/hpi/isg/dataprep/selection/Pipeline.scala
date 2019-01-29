package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.context.DataContext
import de.hpi.isg.dataprep.metadata._
import de.hpi.isg.dataprep.model.repository.{MetadataRepository, ProvenanceRepository}
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog
import de.hpi.isg.dataprep.model.target.objects.{Metadata, TableMetadata}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._

class Pipeline(val dataContext: DataContext, preparations: Preparation[_ <: Preparator]*) {
  val name = "default-pipeline"
  val dataSetName: String = dataContext.getDialect.getTableName
  val rawData: Dataset[Row] = dataContext.getDataFrame
  val metadataRepository: MetadataRepository = initMetadataRepository()
  val provenanceRepository = new ProvenanceRepository


  /**
    * Check whether there are pipeline syntax errors during compilation before starting to execute the pipeline.
    * If there is at least one error, return by throwing a {@link PipelineSyntaxErrorException}, otherwise clear the metadata repository.
    *
    */
  def checkPipelineErrors(): PipelineCheckingResult = {
    val errorsLog = preparations.map(_.checkPipelineErrorWithPrevious(metadataRepository)).collect {
      case p: PipelineErrorLog => p
    }

    if (errorsLog.nonEmpty) PipelineSyntaxError(errorsLog) else PipelineCheckingSuccess
  }

  /**
    * Entrance of the execution of the data preparation pipeline.
    *
    * @throws Exception
    */
  def executePipeline(): Dataset[Row] = preparations.foldLeft(rawData)((data, prep) => prep.preparator.execute(data))

  /**
    * Insert the metadata whose values are already known into the {@link MetadataRepository}
    */
  def initMetadataRepository(): MetadataRepository = {
    val dialect = this.dataContext.getDialect

    val delimiter = new Delimiter(dialect.getDelimiter, new TableMetadata(dialect.getTableName))
    val quoteCharacter = new QuoteCharacter(dialect.getQuoteChar, new TableMetadata(dialect.getTableName))
    val escapeCharacter = new EscapeCharacter(dialect.getEscapeChar, new TableMetadata(dialect.getTableName))
    val headerExistence = new HeaderExistence(dialect.getHasHeader == "true", new TableMetadata(dialect.getTableName))


    val structType = this.rawData.schema
    val fieldMetaData = structType.fields.toList.map(field => new PropertyDataType(field.name, de.hpi.isg.dataprep.util.DataType.getTypeFromSparkType(field.dataType)))

    val initMetadata = List[Metadata](delimiter, quoteCharacter, escapeCharacter, headerExistence) ++ fieldMetaData
    val rep = new MetadataRepository()
    rep.updateMetadata(initMetadata.asJavaCollection)
    rep
  }

  /**
    * Configure the {@link de.hpi.isg.dataprep.model.target.objects.Metadata} prerequisites used when checking metadata, as well as
    * the set of metadata that will be modified after executing a preparator successfully.
    */
  def buildMetadataSetup(): MetadataRepository = ???
}


trait PipelineCheckingResult

case object PipelineCheckingSuccess extends PipelineCheckingResult

case class PipelineSyntaxError(errors: Seq[PipelineErrorLog]) extends PipelineCheckingResult
