package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.object.Metadata;
import de.hpi.isg.dataprep.model.target.object.OperatedObject;

import java.util.Date;

/**
 * {@link CreatedTime} specifies the time that this data file is created.
 *
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class CreatedTime extends Metadata {

    private OperatedObject target;

    private final String name = "created-time";

    private Date createdDateTime;

    public CreatedTime(Date createdDateTime) {
        this.createdDateTime = createdDateTime;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public String getTargetName() {
        // it should return an operated object.
        return null;
    }

    public Date getCreatedDateTime() {
        return createdDateTime;
    }

    @Override
    public String getName() {
        return name;
    }
}
