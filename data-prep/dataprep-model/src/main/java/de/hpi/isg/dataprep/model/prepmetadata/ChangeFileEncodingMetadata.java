package de.hpi.isg.dataprep.model.prepmetadata;


import de.hpi.isg.dataprep.util.MetadataEnum;

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
        prerequisites.add(MetadataEnum.FILE_ENCODING);
        prerequisites.add(MetadataEnum.QUOTE_CHARACTER);
        prerequisites.add(MetadataEnum.DELIMITER);
        prerequisites.add(MetadataEnum.ESCAPE_CHARACTERS);
    }

    @Override
    protected void setToChangeMetadata() {

    }
}
