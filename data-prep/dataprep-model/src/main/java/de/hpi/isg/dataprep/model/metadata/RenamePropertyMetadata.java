package de.hpi.isg.dataprep.model.metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/19
 */
public class RenamePropertyMetadata extends PreparatorMetadata {

    private static RenamePropertyMetadata instance = new RenamePropertyMetadata();

    private RenamePropertyMetadata() {

    }

    public static RenamePropertyMetadata getInstance() {
        return instance;
    }

    @Override
    protected void setPrerequisiteMetadata() {

    }

    @Override
    protected void setToChangeMetadata() {

    }
}
