package de.hpi.isg.dataprep.model.metadata;

import de.hpi.isg.dataprep.util.Metadata;

import java.util.ArrayList;

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
        prerequisites.add(Metadata.QUOTE_CHARACTER);
        prerequisites.add(Metadata.DELIMITER);
        prerequisites.add(Metadata.ESCAPE_CHARACTERS);
        prerequisites.add(Metadata.PROPERTY_DATATYPE);
    }

    @Override
    protected void setToChangeMetadata() {
        toChange.add(Metadata.PROPERTY_DATATYPE);
    }
}
