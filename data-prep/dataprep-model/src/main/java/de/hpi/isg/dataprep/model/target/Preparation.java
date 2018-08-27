package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.exceptions.PipelineSyntaxErrorException;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.MetadataEnum;

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
    public final void checkPipelineErrorWithPrevious(Preparation that) throws PipelineSyntaxErrorException {
        Preparator preparator = this.preparator;
        // preparator has its specific implementation of metadata checking method.

//        for (MetadataEnum metadata : preparator.getPrerequisiteName().keySet()) {
//            // get the prerequisite metadata value of this preparator
//            String metadataVal = preparator.getPrerequisiteName().get(metadata);
//
//            // this metadata does not have a value
//            if (metadataVal == null) {
//                throw new PipelineSyntaxErrorException(String.format("MetadataEnum %s not found.", metadata));
//            }
//
//            // check for this metadata whether preparator has it too, and whether the value fulfills.
//            String outputOfPrevious = that.getPreparator().getToChangeName().get(metadata);
//            if (!outputOfPrevious.equals(metadataVal)) {
//                throw new PipelineSyntaxErrorException(String.format("MetadataEnum: (%s) is not consistent with the previous updated.", metadata.getMetadata()));
//            }
//        }
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
