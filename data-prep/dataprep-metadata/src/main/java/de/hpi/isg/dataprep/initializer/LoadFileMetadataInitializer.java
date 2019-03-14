package de.hpi.isg.dataprep.initializer;

import de.hpi.isg.dataprep.model.metadata.MetadataInitializer;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;

/**
 * @author Lan Jiang
 * @since 2019-03-14
 */
public class LoadFileMetadataInitializer extends MetadataInitializer {

    private final String metadataFilePath;

    public LoadFileMetadataInitializer(String metadataFilePath, MetadataRepository metadataRepository) {
        this.metadataFilePath = metadataFilePath;
        this.metadataRepository = metadataRepository;
    }

    @Override
    public void initializeMetadataRepository() {

    }
}
