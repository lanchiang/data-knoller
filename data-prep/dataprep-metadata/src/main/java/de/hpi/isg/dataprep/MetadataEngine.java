package de.hpi.isg.dataprep;

import de.hpi.isg.dataprep.model.metadata.MetadataInitializer;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.system.Engine;

import java.util.Collection;

/**
 * This engine conducts all the operations related to metadata manipulation.
 *
 * @author lan.jiang
 * @since 12/18/18
 */
public class MetadataEngine implements Engine {

    /**
     * Initialize a metadata repository with the metadata defined in the initializer.
     * @return the created metadata repository
     */
    public static MetadataRepository initMetadataRepository(MetadataInitializer metadataInitializer) {
        metadataInitializer.initializeMetadataRepository();
        MetadataRepository metadataRepository = metadataInitializer.getMetadataRepository();
        return metadataRepository;
    }
}
