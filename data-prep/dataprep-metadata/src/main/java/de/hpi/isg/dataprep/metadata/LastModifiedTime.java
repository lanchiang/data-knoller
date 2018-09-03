package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.object.Metadata;
import de.hpi.isg.dataprep.model.target.object.OperatedObject;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class LastModifiedTime extends Metadata {

    private final String name = "last-modified-time";

    private OperatedObject target;

    private String lastModifiedDateTime;

    public LastModifiedTime(String lastModifiedDateTime) {
        this.lastModifiedDateTime = lastModifiedDateTime;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public String getTargetName() {
        return null;
    }

    public String getLastModifiedDateTime() {
        return lastModifiedDateTime;
    }

    @Override
    public String getName() {
        return name;
    }
}
