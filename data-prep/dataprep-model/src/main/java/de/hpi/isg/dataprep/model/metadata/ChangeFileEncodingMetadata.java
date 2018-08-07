package de.hpi.isg.dataprep.model.metadata;

import de.hpi.isg.dataprep.model.target.Metadata;

import java.util.ArrayList;

/**
 * @author Lan Jiang
 * @since 2018/8/6
 */
public class ChangeFileEncodingMetadata extends PrerequisiteMetadata {

    public ChangeFileEncodingMetadata() {
        prerequisites = new ArrayList<>();
        setMetadata();
    }

    @Override
    protected void setMetadata() {
        prerequisites.add(Metadata.FILE_ENCODING);
    }
}
