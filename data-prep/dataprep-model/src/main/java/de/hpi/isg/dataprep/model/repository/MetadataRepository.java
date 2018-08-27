package de.hpi.isg.dataprep.model.repository;

import de.hpi.isg.dataprep.model.target.Metadata;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class MetadataRepository {

    private Set<Metadata> metadataPool;

    public MetadataRepository() {
        metadataPool = new HashSet<>();
    }

    public Set<Metadata> getMetadataPool() {
        return metadataPool;
    }

    public void updateMetadata(Metadata metadata) {
        if (!metadataPool.contains(metadata)) {
            metadataPool.add(metadata);
        } else {
            Set<Metadata> tmpMetadataPool = new HashSet<>();
            tmpMetadataPool.add(metadata);
            tmpMetadataPool.addAll(metadataPool);
            metadataPool = tmpMetadataPool;
        }
    }

    public void updateMetadata(Collection<Metadata> metadataCollection) {
        metadataCollection.stream().forEach(metadata -> updateMetadata(metadata));
    }
}
