package de.hpi.isg.dataprep.model.target;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;

/**
 * A preparation is a transformation step within a data preparation pipeline.
 * It includes the preparator to be executed in this step, along with its parameters.
 *
 * @author Lan Jiang
 * @since 2018/8/3
 */
public class Preparation {

    private Preparator preparator;
    private Consequences consequences;

    private Pipeline pipeline;

    public Preparation(Preparator preparator) {
        this.preparator = preparator;
        this.preparator.setPreparation(this);
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
