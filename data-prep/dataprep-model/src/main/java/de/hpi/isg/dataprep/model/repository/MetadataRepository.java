package de.hpi.isg.dataprep.model.repository;

import de.hpi.isg.dataprep.Metadata;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class represent the repository of metadata of a {@link AbstractPipeline}.
 *
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class MetadataRepository implements AbstractMetadataRepository, Serializable {

    // the set of metadata this metadata repository is holding currently.
//    private Set<MetadataOld> metadataPool;
    private Set<Metadata> metadataPool;

    public MetadataRepository() {
        metadataPool = new HashSet<>();
    }

    @Override
    public void update(Metadata metadata) {
        if (!metadataPool.contains(metadata)) {
            metadataPool.add(metadata);
        } else {
            metadataPool.remove(metadata);
            metadataPool.add(metadata);
        }
    }

    @Override
    public void add(Metadata metadata) {
        metadataPool.add(metadata);
    }

    @Override
    public void remove(Metadata metadata) {
        metadataPool.remove(metadata);
    }

    @Override
    public void update(Collection<Metadata> metadataCollection) {
        metadataCollection.stream().forEach(metadata -> update(metadata));
    }

    @Override
    public boolean isExist(Metadata metadata) {
        return metadataPool.contains(metadata);
    }

    @Override
    public boolean contains(Metadata metadata) {
        return metadataPool.contains(metadata);
    }

    @Override
    public Metadata getMetadata(Metadata metadata) {
        Optional<Metadata> first = metadataPool.stream().filter(metadataCase -> metadataCase.equals(metadata)).findFirst();
        return first.orElse(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Metadata> List<T> getMetadataOfType(Class<T> clazz) {
        return metadataPool.stream().filter(clazz::isInstance).map(metadata -> (T)metadata).collect(Collectors.toList());
    }

    @Override
    public void clear() {
        metadataPool.clear();
    }
}
