package de.hpi.isg.dataprep;

import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.system.Engine;

import java.util.ArrayList;
import java.util.List;

/**
 * This engine is due to conduct all the operations related to metadata manipulation.
 *
 * @author lan.jiang
 * @since 12/18/18
 */
public class MetadataEngine implements Engine {

    public List<Metadata> checkMetadataPrerequisite(List<Metadata> prerequisites, MetadataRepository metadataRepository) {
        /**
         * check for each metadata whether valid. Valid metadata are those with correct values.
         * Stores invalid ones.
         * If all prerequisiteName are met, read and store all these metadata, used in preparator execution.
         */

        //Added dependency on model.metadata repository @Gerardo
//        MetadataRepository metadataRepository = this.getPreparation().getPipeline().getMetadataRepository();

        List<Metadata> invalid = new ArrayList<>();

        // but if not match invalid.
        prerequisites.stream()
                .forEach(metadata -> {
                    Metadata that = metadataRepository.getMetadata(metadata);
                    if (that == null || !metadata.equalsByValue(that)) {
                        invalid.add(metadata);
                    }
                });
        // not found, add.
//        prerequisite.stream()
//                .filter(metadata -> !metadataRepository.getMetadataPool().contains(metadata))
//                .forEach(metadata -> this.getPreparation().getPipeline().getMetadataRepository().updateMetadata(metadata));
        return invalid;
    }
}
