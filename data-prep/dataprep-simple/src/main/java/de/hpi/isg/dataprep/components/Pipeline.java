package de.hpi.isg.dataprep.components;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.exceptions.PipelineSyntaxErrorException;
import de.hpi.isg.dataprep.initializer.ManualMetadataInitializer;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.error.PropertyError;
import de.hpi.isg.dataprep.model.error.RecordError;
import de.hpi.isg.dataprep.model.metadata.MetadataInitializer;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.write.FlatFileWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
    private Dataset<Row> dataset;

    private FileLoadDialect dialect;

    public Pipeline(Dataset<Row> dataset) {
        this.dataset = dataset;
    }

    public Pipeline(String name, Dataset<Row> dataset) {
        this(dataset);
        this.name = name;
    }

    public Pipeline(DataContext dataContext) {
        this(dataContext.getDataFrame());
        this.dialect = dataContext.getDialect();
        this.schemaMapping = dataContext.getSchemaMapping();
        this.targetMetadata = dataContext.getTargetMetadata();

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
            executePreparation(preparation);
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
        // Delegate this job to a MetadataInitializer.
        MetadataInitializer metadataInitializer = new ManualMetadataInitializer(getDialect(), getDataset());
        metadataInitializer.initializeMetadataRepository();
        metadataRepository = metadataInitializer.getMetadataRepository();
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
        executePreparation(preparation);
        return true;
    }

    /**
     * Execute the recommended preparator that is added into this pipeline. Followed by this execution, data, metadata
     * and other dynamic information must be updated.
     */
    private void executePreparation(AbstractPreparation preparation) {
        ExecutionContext executionContext;
        //execute the added preparation
        try {
            executionContext = preparation.getAbstractPreparator().execute(this.dataset);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        recordErrorLog(executionContext, preparation);
        setDataset(executionContext.newDataFrame());
//        UpdateUtils.updateMetadata(this, preparation.getAbstractPreparator());
        this.updateMetadataRepository(preparation.getAbstractPreparator().getUpdateMetadata());
        recordProvenance();
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
    public Dataset<Row> getDataset() {
        return dataset;
    }

    @Override
    public void setDataset(Dataset<Row> dataset) {
        this.dataset = dataset;
    }

    @Override
    public FileLoadDialect getDialect() {
        return this.dialect;
    }

    @Override
    public void setDialect(FileLoadDialect dialect) {
        this.dialect = dialect;
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
    public void updateMetadataRepository(Collection<Metadata> coming) {
        metadataRepository.updateMetadata(coming);
    }

    @Override
    public void updateTargetMetadata(Collection<Metadata> coming) {
        coming.stream().forEach(metadata -> {
            targetMetadata.remove(metadata);
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
