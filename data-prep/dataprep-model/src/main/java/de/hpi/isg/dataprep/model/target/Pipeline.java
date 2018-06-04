package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.repository.ProvenanceRepository;
import de.hpi.isg.dataprep.util.PipelineExecutable;

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
    private List<Step> steps;

    public Pipeline() {
        this.metadataRepository = new MetadataRepository();
        this.provenanceRepository = new ProvenanceRepository();
        pipelineErrors = new ArrayList<Errorlog>();
        this.steps = new ArrayList<Step>();
    }

    public void createStep(Preparator preparator) {
        Step step = new Step(preparator);
        addStep(step);
    }

    public void addStep(Step step) {
        this.steps.add(step);
    }

    public void checkPipelineErrors() {
        for (Step step : steps) {
            // detect all the pipeline errors for the pipeline from the begin to the current step.
            detectPipelineErrors(steps.subList(0, steps.indexOf(step)));
        }
    }

    private List<Errorlog> detectPipelineErrors(List<Step> forepartSteps) {
        List<Errorlog> errorlogs = new ArrayList<Errorlog>();
        return errorlogs;
    }

    @Override
    public void executePipeline() {
        checkPipelineErrors();
        for (Step step : steps) {
            // do the real execution for each step in the pipeline.
        }
    }
}
