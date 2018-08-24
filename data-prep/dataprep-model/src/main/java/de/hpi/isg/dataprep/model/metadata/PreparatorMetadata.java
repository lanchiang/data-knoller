package de.hpi.isg.dataprep.model.metadata;

import de.hpi.isg.dataprep.util.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents the {@link Metadata} a {@link Preparator} requires for executing and updates after execution.
 *
 * @author Lan Jiang
 * @since 2018/8/24
 */
abstract public class PreparatorMetadata {

    protected List<Metadata> prerequisites;

    /**
     * The list of {@link Metadata} that are changed when a preparator is successfully executed.
     */
    protected List<Metadata> toChange;

    public PreparatorMetadata() {
        prerequisites = new ArrayList<>();
        toChange = new ArrayList<>();
        setPrerequisiteMetadata();
        setToChangeMetadata();
    }

    public List<Metadata> getPrerequisites() {
        return prerequisites;
    }

    public List<Metadata> getToChange() {
        return toChange;
    }

    abstract protected void setPrerequisiteMetadata();
    abstract protected void setToChangeMetadata();
}
