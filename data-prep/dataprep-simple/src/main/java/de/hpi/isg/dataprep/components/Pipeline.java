package de.hpi.isg.dataprep.components;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.exceptions.PipelineSyntaxErrorException;
import de.hpi.isg.dataprep.metadata.*;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.error.PropertyError;
import de.hpi.isg.dataprep.model.error.RecordError;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.repository.ProvenanceRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.objects.TableMetadata;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.utils.UpdateUtils;
import de.hpi.isg.dataprep.write.FlatFileWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 2018/9/10
 */
public class Pipeline implements AbstractPipeline {

    private String name = "default-pipeline";

    /**
     * Specifies the maximum cardinality of a column combination that is passed as the parameter to preparators. Could be moved to the controller.
     * Allows use all columns if the value is Integer.MIN_VALUE.
     */
    private final static int MAX_CARDINALITY = Integer.MIN_VALUE;

    private MetadataRepository metadataRepository = new MetadataRepository();
    private ProvenanceRepository provenanceRepository = new ProvenanceRepository();
    private ErrorRepository errorRepository = new ErrorRepository();

    private List<AbstractPreparation> preparations = new LinkedList<>();

    private DecisionEngine decisionEngine = DecisionEngine.getInstance();

    private int index = 0;

    // Note: just for the grand task test. Ideally, the schema mapping instance should not be held by something else, such as a higher level sand box.
    private SchemaMapping schemaMapping;
    private Set<Metadata> targetMetadata;

    /**
     * The raw data contains a set of {@link Row} instances. Each instance represent a line in a tabular data without schema definition,
     * i.e., each instance has only one attribute that represent the whole line, including content and utility characters.
     */
    private Dataset<Row> rawData;

    private DataContext dataContext;
    private String datasetName;


    public Pipeline(Dataset<Row> rawData) {
        this.rawData = rawData;
    }

    public Pipeline(String name, Dataset<Row> rawData) {
        this(rawData);
        this.name = name;
    }

    public Pipeline(DataContext dataContext) {
        this(dataContext.getDataFrame());
        this.dataContext = dataContext;
        this.schemaMapping = dataContext.getSchemaMapping();
        this.targetMetadata = dataContext.getTargetMetadata();

        this.datasetName = dataContext.getDialect().getTableName();

        // initialize and configure the pipeline.
        initPipeline();
    }

    @Override
    public void initPipeline() {
        // calculate metadata and populate the metadata repository with them.
        initMetadataRepository();
    }

    @Override
    public void addPreparation(AbstractPreparation preparation) {
        preparation.setPipeline(this);
        preparation.setPosition(index++);

//        preparation.getAbstractPreparator().buildMetadataSetup();
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
        // first time initialize metadata repository to check pipeline syntax errors.
        initMetadataRepository();

        buildMetadataSetup();

        try {
            checkPipelineErrors();
        } catch (PipelineSyntaxErrorException e) {
            // write the errorlog to disc.
            FlatFileWriter<ErrorRepository> flatFileWriter = new FlatFileWriter<>();
            flatFileWriter.write(this.getErrorRepository());
            throw e;
        }

        // here optimize the pipeline.

        // second time initialize metadata repository for preparation to execute the pipeline.
        initMetadataRepository();

        // execute the pipeline
        for (AbstractPreparation preparation : preparations) {
            ExecutionContext executionContext = preparation.getAbstractPreparator().execute();

            recordErrorLog(executionContext, preparation);
            this.setRawData(executionContext.newDataFrame());
            UpdateUtils.updateMetadata(this, preparation.getAbstractPreparator());
            recordProvenance();
        }
    }

    private void recordProvenance() {

    }

    /**
     * Call this method whenever an error occurs during the preparator execution in order to
     * record an error log.
     */
    private void recordErrorLog(ExecutionContext executionContext, AbstractPreparation preparation) {
        List<ErrorLog> errorLogs = executionContext.errorsAccumulator().value().stream()
                .map(error -> {
                    ErrorLog errorLog = null;
                    switch (error.getErrorLevel()) {
                        case RECORD: {
                            RecordError pair = (RecordError) error;
                            String value = pair.getErrRecord();
                            Throwable exception = pair.getError();
                            errorLog = new PreparationErrorLog(preparation, value, exception);
                            break;
                        }
                        case PROPERTY: {
                            PropertyError pair = (PropertyError) error;
                            String value = pair.getProperty();
                            Throwable throwable = pair.getThrowable();
                            errorLog = new PreparationErrorLog(preparation, value, throwable);
                            break;
                        }
                        case DATASET: {
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                    return errorLog;
                }).collect(Collectors.toList());
        this.errorRepository.addErrorLogs(errorLogs);
    }


    @Override
    public void initMetadataRepository() {
        FileLoadDialect dialect = this.dataContext.getDialect();

        Delimiter delimiter = new Delimiter(dialect.getDelimiter(), new TableMetadata(dialect.getTableName()));
        QuoteCharacter quoteCharacter = new QuoteCharacter(dialect.getQuoteChar(), new TableMetadata(dialect.getTableName()));
        EscapeCharacter escapeCharacter = new EscapeCharacter(dialect.getEscapeChar(), new TableMetadata(dialect.getTableName()));
        HeaderExistence headerExistence = new HeaderExistence(dialect.getHasHeader().equals("true"), new TableMetadata(dialect.getTableName()));

        List<Metadata> initMetadata = new ArrayList<>();
        initMetadata.add(delimiter);
        initMetadata.add(quoteCharacter);
        initMetadata.add(escapeCharacter);
        initMetadata.add(headerExistence);

        StructType structType = this.rawData.schema();
//        Arrays.stream(structType.fields()).forEach(field -> {
//            DataType dataType = field.dataType();
//            String fieldName = field.name();
//            PropertyDataType propertyDataType = new PropertyDataType(fieldName, de.hpi.isg.dataprep.util.DataType.getTypeFromSparkType(dataType));
//            initMetadata.add(propertyDataType);
//        });

        List<Attribute> attributes = new LinkedList<>();
        Arrays.stream(structType.fields()).forEach(field -> {
            DataType dataType = field.dataType();
            String fieldName = field.name();
            PropertyDataType propertyDataType = new PropertyDataType(fieldName, de.hpi.isg.dataprep.util.DataType.getTypeFromSparkType(dataType));
            Attribute attribute = new Attribute(field);
            attributes.add(attribute);
            initMetadata.add(propertyDataType);
        });
        Schemata schemata = new Schemata("table", attributes);
        initMetadata.add(schemata);

        this.metadataRepository.updateMetadata(initMetadata);
    }

    @Override
    public void buildMetadataSetup() {
        // build preparation, i.e., call the buildpreparator method of preparator instance to set metadata prerequiste and post-change
        this.preparations.stream().forEachOrdered(preparation -> preparation.getAbstractPreparator().buildMetadataSetup());
    }

    @Override
    public boolean addRecommendedPreparation() {
        if (decisionEngine.stopProcess(this)) {
            return false;
        }

        // call the decision engine to collect scores from all preparator candidates, and select the one with the highest score.
        // now the process terminates when the selectBestPreparator method return null.
        AbstractPreparator recommendedPreparator = decisionEngine.selectBestPreparator(this);

        // return a null means the decision engine determines to stop the process
        if (recommendedPreparator == null) {
            throw new RuntimeException("Internal error. Decision engine fails to select the best preparator.");
        }

        // Note: the traditional control flow is to add the preparations first and then execute the batch.
        // While in the recommendation mode the preparator is executed immediately after generated so that the datasets, metadata, env
        // can be updated.
        AbstractPreparation preparation = new Preparation(recommendedPreparator);

        preparation.setPipeline(this);
        preparation.setPosition(index++);
        preparations.add(preparation);
        executeRecommendedPreparation(preparation);
        return true;
    }

    /**
     * Execute the recommended preparator that is added into this pipeline. Followed by this execution, data, metadata
     * and other dynamic information must be updated.
     */
    private void executeRecommendedPreparation(AbstractPreparation preparation) {
        //execute the added preparation
        try {
            preparation.getAbstractPreparator().execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // update the schemaMapping and target metadata
        UpdateUtils.updateSchemaMapping(schemaMapping, preparation.getExecutionContext());
        UpdateUtils.updateMetadata(this, preparation.getAbstractPreparator());
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
    public String getDatasetName() {
        return this.datasetName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public SchemaMapping getSchemaMapping() {
        return this.schemaMapping;
    }

    @Override
    public Set<Metadata> getTargetMetadata() {
        return this.targetMetadata;
    }

    @Override
    public void updateTargetMetadata(Collection<Metadata> coming) {
        coming.stream().forEach(metadata -> {
            if (targetMetadata.contains(metadata)) {
                targetMetadata.remove(metadata);
            }
            targetMetadata.add(metadata);
        });
    }

    @Override
    public void print() {
        System.out.println(this.preparations);
    }

    @Override
    public String toString() {
        return "Pipeline{" +
                "name='" + name + '\'' +
                '}';
    }
}
