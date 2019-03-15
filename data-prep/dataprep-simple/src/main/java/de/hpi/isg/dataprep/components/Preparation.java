package de.hpi.isg.dataprep.components;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.exceptions.DuplicateMetadataException;
import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.exceptions.MetadataNotMatchException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;

import java.util.List;

/**
 * A concrete class of {@link AbstractPreparation} that wraps {@link AbstractPreparator}
 *
 * @author Lan Jiang
 * @since 2018/9/10
 */
public class Preparation implements AbstractPreparation {

    private String name;

    private AbstractPreparator abstractPreparator;

    private int position;

    private AbstractPipeline pipeline;

    public Preparation(AbstractPreparator abstractPreparator) {
        this.abstractPreparator = abstractPreparator;
        this.abstractPreparator.setPreparation(this);

        this.name = this.abstractPreparator.getClass().getSimpleName();
    }

    @Override
    public void checkPipelineErrorWithPrevious(MetadataRepository metadataRepository) {
        if (position != 0) {
            // for each metadata in the prerequisite set of this abstractPreparator, check whether its value agrees with that in the repository
            for (Metadata metadata : abstractPreparator.getPrerequisiteMetadata()) {
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
        List<Metadata> toChangeMetadata = abstractPreparator.getUpdateMetadata();

        /**
         * update metadata repository. This shall be done even if the metadata fail to agree, because the following
         * preparations need to check the pipeline error with this presumably correct metadata. actually we need to
         * update the metadata repository with the toChange list.
         */
        pipeline.updateMetadataRepository(toChangeMetadata);
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
    public AbstractPreparator getAbstractPreparator() {
        return abstractPreparator;
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
