package de.hpi.isg.dataprep.model.repository;

import de.hpi.isg.dataprep.model.target.objects.Metadata;
import scala.Serializable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * This class represent the repository of metadata of a {@link de.hpi.isg.dataprep.model.target.system.AbstractPipeline}.
 *
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class MetadataRepository implements Serializable {

    // the set of metadata this metadata repository is holding currently.
    private Set<Metadata> metadataPool;

    public MetadataRepository() {
        metadataPool = new HashSet<>();
    }

    public Set<Metadata> getMetadataPool() {
        return metadataPool;
    }

    /**
     * Update the metadata pool with the given metadata. If the pool does not contain this metadata, add a new one. If the pool contains it, then first delete the old one, and
     * the add the given metadata. Note that {@link Metadata} is compared only by its name and its scope, except for its value.
     *
     * @param metadata is the metadata to update the metadata pool.
     */
    public void update(Metadata metadata) {
        metadata.setBelongs(this);

        if (!metadataPool.contains(metadata)) {
            metadataPool.add(metadata);
        } else {
            metadataPool.remove(metadata);
            metadataPool.add(metadata);
        }
    }

    /**
     * Add a metadata to the metadata repository.
     *
     * @param metadata is the metadata to be added.
     */
    public void add(Metadata metadata) {
        metadataPool.add(metadata);
    }

    /**
     * Remove the given metadata from the repository.
     *
     * @param metadata is the metadata to be removed.
     */
    public void remove(Metadata metadata) {
        metadataPool.remove(metadata);
    }

    // Todo: this should be removed when all the usage are replaced by the cognominal function in the metadata engine.
    @Deprecated
    public void update(Collection<Metadata> metadataCollection) {
        metadataCollection.stream().forEach(metadata -> update(metadata));
    }

    /**
     * Return true if the repository contains the metadata with the same name regardless of the values of the metadata.
     *
     * @param metadata is the metadata to be checked.
     * @return true if the repository contains the metadata with the same name regardless of the values of the metadata.
     */
    public boolean isExist(Metadata metadata) {
        return metadataPool.contains(metadata);
    }

    /**
     * Return true if the metadata repository contains the metadata whose values are the same as those of the given metadata.
     * @param metadata is the metadata to be checked.
     * @return true if the metadata repository contains the metadata whose values are the same as those of the given metadata.
     */
    public boolean containByValue(Metadata metadata) {
        Optional<Metadata> result = metadataPool.stream().filter(metadata_ -> metadata_.equalsByValue(metadata)).findFirst();
        return result.isPresent();
    }

    /**
     * Get the metadata with the same signature from the repository. Evaluating same signatures inspects whether scope and name are the same.
     *
     * @param metadata
     * @return
     */
    public Metadata getMetadata(Metadata metadata) {
        Optional<Metadata> first = metadataPool.stream().filter(metadata_ -> metadata_.equals(metadata)).findFirst();
        return first.orElse(null);
    }

    public void reset() {
        metadataPool.clear();
    }
}
