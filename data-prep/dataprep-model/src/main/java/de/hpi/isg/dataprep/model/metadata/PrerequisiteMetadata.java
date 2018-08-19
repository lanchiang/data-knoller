package de.hpi.isg.dataprep.model.metadata;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/6
 */
abstract public class PrerequisiteMetadata {

    protected List<String> prerequisites;

    public PrerequisiteMetadata() {
        this.prerequisites = new ArrayList<>();
        setMetadata();
    }

    abstract protected void setMetadata();

    public List<String> getPrerequisites() {
        return prerequisites;
    }
}
