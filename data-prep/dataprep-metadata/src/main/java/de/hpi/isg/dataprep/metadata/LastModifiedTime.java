package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.Metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class LastModifiedTime extends Metadata {

    private final String name = "last-modified-time";

    private String lastModifiedDateTime;

    public LastModifiedTime(String lastModifiedDateTime) {
        this.lastModifiedDateTime = lastModifiedDateTime;
    }

    @Override
    public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException {

    }

    public String getLastModifiedDateTime() {
        return lastModifiedDateTime;
    }

    @Override
    public String getName() {
        return name;
    }
}
