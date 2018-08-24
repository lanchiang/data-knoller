package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.Metadata;

/**
 * A preparation is a transformation step within a data preparation pipeline.
 * It includes the preparator to be executed in this step, along with its parameters.
 *
 * @author Lan Jiang
 * @since 2018/8/3
 */
public class Preparation extends PipelineComponent {

    private Preparator preparator;
    private Consequences consequences;

    private Pipeline pipeline;

    public Preparation(Preparator preparator) {
        this.preparator = preparator;
        this.preparator.setPreparation(this);
    }

    /**
     * Check whether this preparator along with the previous one cause pipeline-level errors.
     *
     * @param that is instance of the previous {@link Preparator} in the {@link de.hpi.isg.dataprep.model.target.Pipeline}.
     * @return true if there is at least one pipeline-level error.
     */
    public final boolean checkPipelineErrorWithPrevious(Preparation that) throws Exception {
        Preparator preparator = this.preparator;
        for (Metadata metadata : preparator.getPrerequisites()) {
            String metadataVal = preparator.getMetadataValue().get(metadata);
            if (metadataVal == null) {
                throw new Exception(String.format("Metadata %s not found.", metadata));
            }

            // check for this metadata whether preparator has it too, and whether the value fulfills.
        }
        return false;
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

    public void setConsequences(Consequences consequences) {
        this.consequences = consequences;
    }

    public Consequences getConsequences() {
        return consequences;
    }
}
