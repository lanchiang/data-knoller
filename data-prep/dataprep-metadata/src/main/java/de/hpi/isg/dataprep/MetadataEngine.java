package de.hpi.isg.dataprep;

import de.hpi.isg.dataprep.model.metadata.MetadataInitializer;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.system.Engine;

import java.util.Collection;

/**
 * This engine conducts all the operations related to metadata manipulation.
 *
 * @author lan.jiang
 * @since 12/18/18
 */
public class MetadataEngine implements Engine {

    public MetadataEngine() {}

    /**
     * Initialize a metadata repository with the metadata defined in the initializer.
     * @return the created metadata repository
     */
    public MetadataRepository initMetadataRepository(MetadataInitializer metadataInitializer) {
        metadataInitializer.initializeMetadataRepository();
        MetadataRepository metadataRepository = metadataInitializer.getMetadataRepository();
        return metadataRepository;
    }

    /**
     * Return true if the metadata repository contains this metadata. Containment does not require the value of the metadata is the same as the counterpart in the repository.
     * @param metadata is the metadata to be checked
     * @return true if the metadata repository contains this metadata
     */
    public boolean isExistInRepository(Metadata metadata, MetadataRepository metadataRepository) {
        return metadataRepository.isExist(metadata);
    }

    /**
     * Return true if the metadata repository has the same metadata as the given. It means the values of the two metadata are the same.
     * @param metadata is the metadata to be checked
     * @return true if the metadata repository has the same metadata as the given.
     */
    public boolean isTheSameInRepository(Metadata metadata, MetadataRepository metadataRepository) {
        return metadataRepository.containByValue(metadata);
    }

    /**
     * Add a metadata to the metadata repository.
     *
     * @param metadata is the metadata to be added.
     * @param metadataRepository is the metadata repository adds the metadata.
     */
    public void addMetadata(Metadata metadata, MetadataRepository metadataRepository) {
        metadataRepository.add(metadata);
    }

    /**
     * Delete the metadata from the metadata repository.
     *
     * @param metadata is the metadata to be deleted.
     * @param metadataRepository is the operating metadata repository
     */
    public void deleteMetadata(Metadata metadata, MetadataRepository metadataRepository) {
        metadataRepository.remove(metadata);
    }

    /**
     * Update a single metadata in the metadata repository.
     * @param metadata is the metadata to update the metadata repository.
     */
    public void updateMetadata(Metadata metadata, MetadataRepository metadataRepository) {
        metadataRepository.update(metadata);
    }

    /**
     * Update a set of metadata in the metadata repository.
     * @param metadataCollection is the set of metadata to update the metadata repository.
     */
    public void updateMetadata(Collection<Metadata> metadataCollection, MetadataRepository metadataRepository) {
        metadataCollection.stream().forEach(metadata -> updateMetadata(metadata, metadataRepository));
    }

    // Todo: invalidate a metadata.
}
