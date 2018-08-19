package de.hpi.isg.dataprep.model.metadata;

import de.hpi.isg.dataprep.model.target.Metadata;

import java.util.ArrayList;

/**
 * @author Lan Jiang
 * @since 2018/8/6
 */
public class ChangeFileEncodingMetadata extends PrerequisiteMetadata {

    @Override
    protected void setMetadata() {
        prerequisites.add(MetadataUtil.FILE_ENCODING);
        prerequisites.add(MetadataUtil.QUOTE_CHARACTER);
        prerequisites.add(MetadataUtil.DELIMITER);
        prerequisites.add(MetadataUtil.ESCAPE_CHARACTERS);
    }
}
