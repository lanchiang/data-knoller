package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;

/**
 * This metadata specifies the existence of headers in a relational data model.
 *
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class HeaderExistence extends Metadata {

    private boolean headerExistence;

    private HeaderExistence() {
        super(HeaderExistence.class.getSimpleName());
    }

    public HeaderExistence(boolean headerExistence, MetadataScope scope) {
        this();
        this.scope = scope;
        this.headerExistence = headerExistence;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return this.equals(metadata);
    }

    public boolean isHeaderExistence() {
        return headerExistence;
    }
}
