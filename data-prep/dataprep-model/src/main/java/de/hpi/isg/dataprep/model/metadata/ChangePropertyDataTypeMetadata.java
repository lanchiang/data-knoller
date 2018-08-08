package de.hpi.isg.dataprep.model.metadata;

import java.util.ArrayList;

/**
 * @author Lan Jiang
 * @since 2018/8/7
 */
public class ChangePropertyDataTypeMetadata extends PrerequisiteMetadata {

    public ChangePropertyDataTypeMetadata() {
        setMetadata();
    }

    @Override
    protected void setMetadata() {
        prerequisites.add(MetadataUtil.QUOTE_CHARACTER);
        prerequisites.add(MetadataUtil.DELIMITER);
        prerequisites.add(MetadataUtil.ESCAPE_CHARACTERS);
        prerequisites.add(MetadataUtil.PROPERTY_DATATYPE);
    }
}
