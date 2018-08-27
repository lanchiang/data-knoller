package de.hpi.isg.dataprep.model.prepmetadata;

/**
 * @author Lan Jiang
 * @since 2018/8/21
 */
public class MovePropertyMetadata extends PreparatorMetadata {

    private static MovePropertyMetadata instance = new MovePropertyMetadata();

    private MovePropertyMetadata() {}

    public static MovePropertyMetadata getInstance() {
        return instance;
    }

    @Override
    protected void setPrerequisiteMetadata() {

    }

    @Override
    protected void setToChangeMetadata() {

    }
}
