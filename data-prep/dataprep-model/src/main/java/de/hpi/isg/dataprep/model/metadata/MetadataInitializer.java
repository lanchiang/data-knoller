package de.hpi.isg.dataprep.model.metadata;

import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;

import java.util.List;

/**
 * This interface provides the APIs for a metadata initializer.
 *
 * @author Lan Jiang
 * @since 2019-03-14
 */
abstract public class MetadataInitializer {

    protected List<Metadata> initMetadata;
    protected MetadataRepository metadataRepository;

    abstract public void initializeMetadataRepository();

    public MetadataRepository getMetadataRepository() {
        return metadataRepository;
    }
}
