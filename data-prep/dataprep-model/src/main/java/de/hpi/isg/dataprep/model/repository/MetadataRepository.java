package de.hpi.isg.dataprep.model.repository;

import de.hpi.isg.dataprep.model.target.Target;
import de.hpi.isg.dataprep.model.target.objects.Metadata;

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

    public void updateMetadata(Metadata metadata) {
        if (!getMetadataByTargetNameString(metadata.getTargetName())) {
            metadataPool.add(metadata);
        } else {
            // If the metadata is in the repository, keep all the other except the metadata sharing the same name with the one in parameter.
            Set<Metadata> tmpMetadataPool = metadataPool.stream()
                    .filter(metadata_ -> !metadata_.getTargetName().equals(metadata.getTargetName())).collect(Collectors.toSet());
            tmpMetadataPool.add(metadata);
            metadataPool = tmpMetadataPool;
        }
    }

    public void updateMetadata(Collection<Metadata> metadataCollection) {
        metadataCollection.stream().forEach(metadata -> updateMetadata(metadata));
    }

    public boolean getMetadataByTargetNameString(String targetName) {
        Optional<Metadata> result = metadataPool.stream().filter(metadata -> metadata.getTargetName().equals(targetName)).findFirst();
        return result.isPresent();
    }

    public boolean getMetadataByTarget(Target target) {
        return true;
    }
}
