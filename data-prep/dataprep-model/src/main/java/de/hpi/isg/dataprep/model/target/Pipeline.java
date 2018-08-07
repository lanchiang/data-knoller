package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.repository.ProvenanceRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class Pipeline extends Target {

    private MetadataRepository metadataRepository;
    private ProvenanceRepository provenanceRepository;
    private List<Errorlog> pipelineErrors;

    private List<Preparation> preparations;
//    private RawData rawData;

    private Dataset<Row> rawData;

    public Pipeline() {
        this.metadataRepository = new MetadataRepository();
        this.provenanceRepository = new ProvenanceRepository();
        this.pipelineErrors = new ArrayList<>();
        this.preparations = new LinkedList<>();
    }

    public Pipeline(Dataset<Row> dataset) {
        this();
        this.rawData = dataset;
    }

    public void addPreparator(Preparation preparation) {
        this.preparations.add(preparation);
    }

    private void checkPipelineErrors() {
        for (Preparation preparation : preparations) {
            // detect all the pipeline errors for the pipeline from the begin to the current step.
            detectPipelineErrors(preparations.subList(0, preparations.indexOf(preparation)));
        }
    }

    private List<Errorlog> detectPipelineErrors(List<Preparation> forepartPipeline) {
        List<Errorlog> errorlogs = new ArrayList<Errorlog>();
        return errorlogs;
    }
    public void executePipeline() throws Exception {
        checkPipelineErrors();
        for (Preparation preparation : preparations) {
            // do the real execution for each step in the pipeline.
            preparation.getPreparator().execute();
        }
    }

    public Dataset<Row> getRawData() {
        return rawData;
    }
}
