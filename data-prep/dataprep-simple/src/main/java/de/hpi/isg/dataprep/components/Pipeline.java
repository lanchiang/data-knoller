package de.hpi.isg.dataprep.components;

import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.exceptions.PipelineSyntaxErrorException;
import de.hpi.isg.dataprep.metadata.Delimiter;
import de.hpi.isg.dataprep.metadata.EscapeCharacter;
import de.hpi.isg.dataprep.metadata.HeaderExistence;
import de.hpi.isg.dataprep.metadata.QuoteCharacter;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.repository.ProvenanceRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.objects.DataSet;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.write.FlatFileWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/9/10
 */
public class Pipeline implements AbstractPipeline {

    private String name = "Default pipeline";

    private MetadataRepository metadataRepository;
    private ProvenanceRepository provenanceRepository;
    private ErrorRepository errorRepository;

    private List<AbstractPreparation> preparations;

    private int index = 0;

    /**
     * The raw data contains a set of {@link Row} instances. Each instance represent a line in a tabular data without schema definition,
     * i.e., each instance has only one attribute that represent the whole line, including content and utility characters.
     */
    private Dataset<Row> rawData;
    private DataContext dataContext;

    private Pipeline() {
        this.metadataRepository = new MetadataRepository();
        this.provenanceRepository = new ProvenanceRepository();
        this.errorRepository = new ErrorRepository();
        this.preparations = new LinkedList<>();
    }

    public Pipeline(Dataset<Row> rawData) {
        this();
        this.rawData = rawData;
    }

    public Pipeline(String name, Dataset<Row> rawData) {
        this(rawData);
        this.name = name;
    }

    public Pipeline(DataContext dataContext) {
        this();
        this.dataContext = dataContext;

    }

    @Override
    public void addPreparation(AbstractPreparation preparation) {
        preparation.setPipeline(this);
        preparation.setPosition(index++);

        // build preparation, i.e., call the buildpreparator method of preparator instance to set metadata prerequiste and post-change
        preparation.getPreparator().buildMetadataSetup();

        this.preparations.add(preparation);
    }

    @Override
    public void checkPipelineErrors() throws PipelineSyntaxErrorException {
        MetadataRepository metadataRepository = this.metadataRepository;

        // the first preparator should not produce pipeline syntax error. Therefore, do not check the prerequisite for it.
        // Only add the toChange list to metadata repository.
        preparations.stream().forEachOrdered(preparation -> preparation.checkPipelineErrorWithPrevious(metadataRepository));

        long numberOfPipelineError = errorRepository.getErrorLogs().stream()
                .filter(errorLog -> errorLog instanceof PipelineErrorLog)
                .map(errorLog -> (PipelineErrorLog) errorLog)
                .count();
        if (numberOfPipelineError > 0) {
            throw new PipelineSyntaxErrorException("The pipeline contains syntax errors.");
        }

        // remove all the metadata assumed during the pipeline error check phase.
        metadataRepository.getMetadataPool().clear();
    }

    @Override
    public void executePipeline() throws Exception {
        initMetadataRepository();
        try {
            checkPipelineErrors();
        } catch (PipelineSyntaxErrorException e) {
            // write the errorlog to disc.
            FlatFileWriter<ErrorLog> flatFileWriter = new FlatFileWriter<>(this.getErrorRepository().getErrorLogs());
            flatFileWriter.write();
            throw e;
        }

        // here optimize the pipeline.

        initMetadataRepository();
        for (AbstractPreparation preparation : preparations) {
            preparation.getPreparator().execute();
        }
    }

    @Override
    public void initMetadataRepository() {
        FileLoadDialect dialect = this.dataContext.getDialect();
//        Delimiter delimiter = new Delimiter(dialect.getDelimiter());
//        QuoteCharacter quoteCharacter = new QuoteCharacter(dialect.getQuoteChar());
//        EscapeCharacter escapeCharacter = new EscapeCharacter(dialect.getEscapeChar());
//        HeaderExistence headerExistence = new HeaderExistence(dialect.getHasHeader().equals("true"));

        Delimiter delimiter = new Delimiter(new DataSet(dialect.getDataSetName()), dialect.getDelimiter());
        QuoteCharacter quoteCharacter = new QuoteCharacter(new DataSet(dialect.getDataSetName()), dialect.getQuoteChar());
        EscapeCharacter escapeCharacter = new EscapeCharacter(new DataSet(dialect.getDataSetName()), dialect.getEscapeChar());
        HeaderExistence headerExistence = new HeaderExistence(new DataSet(dialect.getDataSetName()), dialect.getHasHeader().equals("true"));

        List<Metadata> initMetadata = new ArrayList<>();
        initMetadata.add(delimiter);
        initMetadata.add(quoteCharacter);
        initMetadata.add(escapeCharacter);
        initMetadata.add(headerExistence);

        this.metadataRepository.updateMetadata(initMetadata);
    }

    @Override
    public List<AbstractPreparation> getPreparations() {
        return preparations;
    }

    @Override
    public ErrorRepository getErrorRepository() {
        return errorRepository;
    }

    @Override
    public MetadataRepository getMetadataRepository() {
        return metadataRepository;
    }

    @Override
    public ProvenanceRepository getProvenanceRepository() {
        return provenanceRepository;
    }

    @Override
    public Dataset<Row> getRawData() {
        return rawData;
    }

    @Override
    public void setRawData(Dataset<Row> rawData) {
        this.rawData = rawData;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Pipeline{" +
                "name='" + name + '\'' +
                '}';
    }
}
