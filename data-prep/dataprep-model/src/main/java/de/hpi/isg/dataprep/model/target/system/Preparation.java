package de.hpi.isg.dataprep.model.target.system;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.object.Metadata;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;

import java.util.List;

/**
 * A preparation is a transformation step within a data preparation pipeline.
 * It includes the preparator to be executed in this step, along with its parameters.
 *
 * @author Lan Jiang
 * @since 2018/8/3
 */
public class Preparation extends PipelineComponent {

    private String name;

    private Preparator preparator;
    private Consequences consequences;

    private int position; // the position of this preparation in the pipeline

    private Pipeline pipeline;

    public Preparation(Preparator preparator) {
        this.preparator = preparator;
        this.preparator.setPreparation(this);

        this.name = this.preparator.getClass().getSimpleName();
    }

    /**
     * Check whether this preparator along with the previous one cause pipeline-level errors.
     *
     * @param metadataRepository is the instance of the metadata repository of this pipeline.
     * @return true if there is at least one pipeline-level error.
     */
    public final void checkPipelineErrorWithPrevious(MetadataRepository metadataRepository) {
        if (position != 0) {
            // for each metadata in the prerequisite set of this preparator, check whether its value agrees with that in the repository
            for (Metadata metadata : preparator.getPrerequisite()) {
                try {
                    metadata.checkMetadata(metadataRepository);
                } catch (RuntimeMetadataException e) {
                    // if the check fails... push a pipeline syntax error exception to a set.
                    this.getPipeline().getErrorRepository().addErrorLog(new PipelineErrorLog(this.getPipeline(), this, e));
                }
            }
        }
        List<Metadata> toChangeMetadata = preparator.getToChange();

        // update metadata repository. This shall be done even if the metadata fail to agree, because the following preparations need to
        // check the pipeline error with this presumably correct metadata.
        // actually we need to update the metadata repository with the toChange list.
        metadataRepository.updateMetadata(toChangeMetadata);
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public Preparator getPreparator() {
        return preparator;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setConsequences(Consequences consequences) {
        this.consequences = consequences;
    }

    public Consequences getConsequences() {
        return consequences;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Preparation{" +
                "name='" + name + '\'' +
                ", position=" + position +
                ", pipeline=" + pipeline +
                '}';
    }
}
