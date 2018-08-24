package de.hpi.isg.dataprep.model.metadata;


import de.hpi.isg.dataprep.util.Metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/6
 */
public class ChangeFileEncodingMetadata extends PreparatorMetadata {

    private static ChangeFileEncodingMetadata instance = new ChangeFileEncodingMetadata();

    private ChangeFileEncodingMetadata() {
        setPrerequisiteMetadata();
        setToChangeMetadata();
    }

    public static ChangeFileEncodingMetadata getInstance() {return instance;}

    @Override
    protected void setPrerequisiteMetadata() {
        prerequisites.add(Metadata.FILE_ENCODING);
        prerequisites.add(Metadata.QUOTE_CHARACTER);
        prerequisites.add(Metadata.DELIMITER);
        prerequisites.add(Metadata.ESCAPE_CHARACTERS);
    }

    @Override
    protected void setToChangeMetadata() {

    }
}
