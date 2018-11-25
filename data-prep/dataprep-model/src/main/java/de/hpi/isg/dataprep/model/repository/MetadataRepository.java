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

    public void updateMetadata(Metadata metadata) {
        metadata.setBelongs(this);

        if (!metadataPool.contains(metadata)) {
            metadataPool.add(metadata);
        } else {
            metadataPool.remove(metadata);
            metadataPool.add(metadata);
        }
    }

    public void updateMetadata(Collection<Metadata> metadataCollection) {
        metadataCollection.stream().forEach(metadata -> updateMetadata(metadata));
    }

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
        if (first.isPresent()) {
            return first.get();
        } else {
            return null;
        }
    }
}
