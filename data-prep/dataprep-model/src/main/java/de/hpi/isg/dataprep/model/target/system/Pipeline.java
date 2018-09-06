package de.hpi.isg.dataprep.model.target.system;

import de.hpi.isg.dataprep.exceptions.PipelineSyntaxErrorException;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.repository.ProvenanceRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.write.FlatFileWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.BufferedWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class Pipeline extends PipelineComponent {

    private String name = "Default pipeline";

    private MetadataRepository metadataRepository;
    private ProvenanceRepository provenanceRepository;
    private ErrorRepository errorRepository;

    private List<Preparation> preparations;

    private int index = 0;

    /**
     * The raw data contains a set of {@link Row} instances. Each instance represent a line in a tabular data without schema definition,
     * i.e., each instance has only one attribute that represent the whole line, including content and utility characters.
     */
    private Dataset<Row> rawData;

    private Pipeline() {
        this.metadataRepository = new MetadataRepository();
        this.provenanceRepository = new ProvenanceRepository();
        this.errorRepository = new ErrorRepository();
        this.preparations = new LinkedList<>();
    }

    public Pipeline(Dataset<Row> dataset) {
        this();
        this.rawData = dataset;
    }

    public Pipeline(String name, Dataset<Row> dataset) {
        this(dataset);
        this.name = name;
    }

    public void addPreparation(Preparation preparation) {
        preparation.setPipeline(this);
        preparation.setPosition(index++);

        // build preparation, i.e., call the buildpreparator method of preparator instance to set metadata prerequiste and post-change
        preparation.getPreparator().buildMetadataSetup();

        this.preparations.add(preparation);
    }

    /**
     * Check whether there are pipeline syntax errors during compilation before starting to execute the pipeline.
     * If there is at least one error, return by throwing a {@link PipelineSyntaxErrorException}, otherwise clear the metadata repository.
     *
     * @throws PipelineSyntaxErrorException
     */
    private void checkPipelineErrors() throws PipelineSyntaxErrorException {
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

    public void executePipeline() throws Exception {
        try {
            checkPipelineErrors();
        } catch (PipelineSyntaxErrorException e) {
            // write the errorlog to disc.
            FlatFileWriter<ErrorLog> flatFileWriter = new FlatFileWriter<>(this.getErrorRepository().getErrorLogs());
            flatFileWriter.write();
            throw e;
        }

        // here optimize the pipeline.
        for (Preparation preparation : preparations) {
            preparation.getPreparator().execute();

            System.out.println("-------------------------------------------------------------------------");
        }
    }

    public List<Preparation> getPreparations() {
        return preparations;
    }

    public ErrorRepository getErrorRepository() {
        return errorRepository;
    }

    public MetadataRepository getMetadataRepository() {
        return metadataRepository;
    }

    public ProvenanceRepository getProvenanceRepository() {
        return provenanceRepository;
    }

    public Dataset<Row> getRawData() {
        return rawData;
    }

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
