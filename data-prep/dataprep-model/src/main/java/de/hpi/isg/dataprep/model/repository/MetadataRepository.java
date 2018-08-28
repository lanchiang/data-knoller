package de.hpi.isg.dataprep.model.repository;

import de.hpi.isg.dataprep.model.target.Metadata;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * This class represent the repository of metadata of a {@link de.hpi.isg.dataprep.model.target.Pipeline}.
 *
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class MetadataRepository<T> {

    private Set<T> metadataPool;

    public MetadataRepository() {
        metadataPool = new HashSet<>();
    }

    public Set<T> getMetadataPool() {
        return metadataPool;
    }

    public void updateMetadata(T metadata) {
        if (!metadataPool.contains(metadata)) {
            metadataPool.add(metadata);
        } else {
            Set<T> tmpMetadataPool = new HashSet<>();
            tmpMetadataPool.add(metadata);
            tmpMetadataPool.addAll(metadataPool);
            metadataPool = tmpMetadataPool;
        }
    }

    public void updateMetadata(Collection<T> metadataCollection) {
        metadataCollection.stream().forEach(metadata -> updateMetadata(metadata));
    }
}
