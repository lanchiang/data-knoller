package de.hpi.isg.dataprep.components;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.exceptions.DuplicateMetadataException;
import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.exceptions.MetadataNotMatchException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.object.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;

import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/9/10
 */
public class Preparation implements AbstractPreparation {

    private String name;

    private Preparator preparator;
    private Consequences consequences;

    private int position;

    private AbstractPipeline pipeline;

    public Preparation(Preparator preparator) {
        this.preparator = preparator;
        this.preparator.setPreparation(this);

        this.name = this.preparator.getClass().getSimpleName();
    }

    @Override
    public void checkPipelineErrorWithPrevious(MetadataRepository metadataRepository) {
        if (position != 0) {
            // for each metadata in the prerequisite set of this preparator, check whether its value agrees with that in the repository
            for (Metadata metadata : preparator.getPrerequisite()) {
                try {
                    metadata.checkMetadata(metadataRepository);
                } catch (MetadataNotFoundException e) {
                    // if metadata not found, add it into the metadata repository.
                    this.getPipeline().getMetadataRepository().updateMetadata(metadata);
                } catch (DuplicateMetadataException | MetadataNotMatchException e) {
                    // if is other Metadata runtime exceptions... push a pipeline syntax error exception to a set.
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

    @Override
    public AbstractPipeline getPipeline() {
        return pipeline;
    }

    @Override
    public void setPipeline(AbstractPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public Preparator getPreparator() {
        return preparator;
    }

    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public void setConsequences(Consequences consequences) {
        this.consequences = consequences;
    }

    @Override
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
