package de.hpi.isg.dataprep.model.prepmetadata;

/**
 * @author Lan Jiang
 * @since 2018/8/20
 */
public class AddPropertyMetadata extends PreparatorMetadata {

    private static AddPropertyMetadata instance = new AddPropertyMetadata();

    private AddPropertyMetadata() {}

    public static AddPropertyMetadata getInstance() {
        return instance;
    }

    @Override
    protected void setPrerequisiteMetadata() {

    }

    @Override
    protected void setToChangeMetadata() {

    }
}
