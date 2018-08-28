package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.Metadata;

/**
 * This metadata specifies the existence of headers in a relational data model.
 *
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class HeaderExistence extends Metadata {

    private final String name = "header-existence";

    private boolean headerExistence;

    public HeaderExistence(boolean headerExistence) {
        this.headerExistence = headerExistence;
    }

    @Override
    public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException {

    }

    public boolean isHeaderExistence() {
        return headerExistence;
    }

    @Override
    public String getName() {
        return name;
    }
}
