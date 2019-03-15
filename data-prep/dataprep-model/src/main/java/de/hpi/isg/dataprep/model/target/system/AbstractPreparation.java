package de.hpi.isg.dataprep.model.target.system;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.util.Nameable;

/**
 * @author Lan Jiang
 * @since 2018/9/10
 */
public interface AbstractPreparation extends Nameable {

    /**
     * Check whether this preparator along with the previous one cause pipeline-level errors.
     *
     * @param metadataRepository is the instance of the metadata repository of this pipeline.
     * @return true if there is at least one pipeline-level error.
     */
    void checkPipelineErrorWithPrevious(MetadataRepository metadataRepository);

    AbstractPipeline getPipeline();

    void setPipeline(AbstractPipeline pipeline);

    AbstractPreparator getAbstractPreparator();

    int getPosition();

    void setPosition(int position);
}
