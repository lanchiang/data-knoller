package de.hpi.isg.dataprep.model.prepmetadata;

import de.hpi.isg.dataprep.util.MetadataEnum;

/**
 * @author Lan Jiang
 * @since 2018/8/7
 */
public class ChangePropertyDataTypeMetadata extends PreparatorMetadata {

    private static ChangePropertyDataTypeMetadata instance = new ChangePropertyDataTypeMetadata();

    private ChangePropertyDataTypeMetadata() {
        super();
    }

    public static ChangePropertyDataTypeMetadata getInstance() {
        return instance;
    }

    @Override
    protected void setPrerequisiteMetadata() {
        prerequisites.add(MetadataEnum.PROPERTY_DATA_TYPE);
    }

    @Override
    protected void setToChangeMetadata() {
        toChange.putIfAbsent(MetadataEnum.PROPERTY_DATA_TYPE, null);
    }
}
