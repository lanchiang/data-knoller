package de.hpi.isg.dataprep.model.metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/20
 */
public class AddPropertyMetadata extends PrerequisiteMetadata {

    private static AddPropertyMetadata instance = new AddPropertyMetadata();

    private AddPropertyMetadata() {}

    public static AddPropertyMetadata getInstance() {
        return instance;
    }

    @Override
    protected void setMetadata() {

    }
}
