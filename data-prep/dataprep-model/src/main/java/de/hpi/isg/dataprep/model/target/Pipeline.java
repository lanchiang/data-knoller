package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.repository.ProvenanceRepository;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class Pipeline extends PipelineComponent {

    private MetadataRepository metadataRepository;
    private ProvenanceRepository provenanceRepository;
    private ErrorRepository errorRepository;

    private List<PipelineErrorLog> pipelineErrors;

    private List<Preparation> preparations;

    /**
     * The raw data contains a set of {@link Row} instances. Each instance represent a line in a tabular data without schema definition,
     * i.e., each instance has only one attribute that represent the whole line, including content and utility characters.
     */
    private Dataset<Row> rawData;

    private Pipeline() {
        this.metadataRepository = new MetadataRepository();
        this.provenanceRepository = new ProvenanceRepository();
        this.errorRepository = new ErrorRepository();
        this.pipelineErrors = new ArrayList<>();
        this.preparations = new LinkedList<>();
    }

    public Pipeline(Dataset<Row> dataset) {
        this();
        this.rawData = dataset;
    }

    public void addPreparation(Preparation preparation) {
        preparation.setPipeline(this);
        this.preparations.add(preparation);
    }

    private void checkPipelineErrors() throws Exception {
        for (Preparation preparation : preparations) {

        }
    }

    public void executePipeline() throws Exception {
        try {
            checkPipelineErrors();
        } catch (Exception e) {
            // do something
        }
        for (Preparation preparation : preparations) {
            preparation.getPreparator().execute();
        }
    }

    public ErrorRepository getErrorRepository() {
        return errorRepository;
    }

    public Dataset<Row> getRawData() {
        return rawData;
    }

    public void setRawData(Dataset<Row> rawData) {
        this.rawData = rawData;
    }
}
