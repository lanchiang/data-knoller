package de.hpi.isg.dataprep.model.prepmetadata;

/**
 * @author Lan Jiang
 * @since 2018/8/20
 */
public class DeletePropertyMetadata extends PreparatorMetadata {

    private static DeletePropertyMetadata instance = new DeletePropertyMetadata();

    private DeletePropertyMetadata() {}

    public static DeletePropertyMetadata getInstance() {
        return instance;
    }

    @Override
    protected void setPrerequisiteMetadata() {

    }

    @Override
    protected void setToChangeMetadata() {

    }
}
