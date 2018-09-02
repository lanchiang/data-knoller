package de.hpi.isg.dataprep.model.prepmetadata;

import de.hpi.isg.dataprep.util.MetadataEnum;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class represents the {@link MetadataEnum} a {@link Preparator} requires for executing and updates after execution.
 *
 * @author Lan Jiang
 * @since 2018/8/24
 */
abstract public class PreparatorMetadata {

//    protected Map<MetadataEnum, String> prerequisiteName;

    protected List<MetadataEnum> prerequisites;

    /**
     * The map of {@link MetadataEnum} that are changed when a preparator is successfully executed.
     */
    protected Map<MetadataEnum, String> toChange;

    public PreparatorMetadata() {
//        prerequisiteName = new HashMap<>();
        prerequisites = new ArrayList<>();
        toChange = new HashMap<>();
        setPrerequisiteMetadata();
        setToChangeMetadata();
    }

    public List<MetadataEnum> getPrerequisites() {
        return prerequisites;
    }

    public Map<MetadataEnum, String> getToChange() {
        return toChange;
    }

    abstract protected void setPrerequisiteMetadata();
    abstract protected void setToChangeMetadata();
}
