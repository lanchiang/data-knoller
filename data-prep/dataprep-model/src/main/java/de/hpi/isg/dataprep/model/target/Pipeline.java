package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.repository.ProvenanceRepository;
import de.hpi.isg.dataprep.util.PipelineExecutable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class Pipeline extends Target implements PipelineExecutable {

    private MetadataRepository metadataRepository;
    private ProvenanceRepository provenanceRepository;
    private List<Errorlog> pipelineErrors;
    private List<Preparator> preparators;
    private Dataset<Row> dataset;

    public Pipeline() {
        this.metadataRepository = new MetadataRepository();
        this.provenanceRepository = new ProvenanceRepository();
        this.pipelineErrors = new ArrayList<>();
        this.preparators = new ArrayList<>();
    }

    public Pipeline(Dataset<Row> dataset) {
        this();
        this.dataset = dataset;
    }

    public void addPreparator(Preparator preparator) {
        this.preparators.add(preparator);
    }

    public void checkPipelineErrors() {
        for (Preparator preparator : preparators) {
            // detect all the pipeline errors for the pipeline from the begin to the current step.
            detectPipelineErrors(preparators.subList(0, preparators.indexOf(preparator)));
        }
    }

    private List<Errorlog> detectPipelineErrors(List<Preparator> forepartPipeline) {
        List<Errorlog> errorlogs = new ArrayList<Errorlog>();
        return errorlogs;
    }

    @Override
    public void executePipeline() {
        checkPipelineErrors();
        for (Preparator preparator : preparators) {
            // do the real execution for each step in the pipeline.
            preparator.execute(dataset);
        }
    }
}
