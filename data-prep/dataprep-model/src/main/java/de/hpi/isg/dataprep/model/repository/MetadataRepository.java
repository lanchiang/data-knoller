package de.hpi.isg.dataprep.model.repository;

import de.hpi.isg.dataprep.model.target.Target;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.Property;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class represent the repository of metadata of a {@link de.hpi.isg.dataprep.model.target.system.AbstractPipeline}.
 *
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class MetadataRepository {

    // the set of metadata this metadata repository is holding currently.
    private Set<Metadata> metadataPool;

    public MetadataRepository() {
        metadataPool = new HashSet<>();
    }

    public Set<Metadata> getMetadataPool() {
        return metadataPool;
    }

    public void setMetadataPool(Set<Metadata> metadataPool) {
        this.metadataPool = metadataPool;
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
}
