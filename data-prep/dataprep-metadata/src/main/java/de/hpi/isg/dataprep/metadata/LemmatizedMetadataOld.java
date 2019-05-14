package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.ColumnMetadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataOld;

public class LemmatizedMetadataOld extends MetadataOld {

    public LemmatizedMetadataOld() {
        super(LemmatizedMetadataOld.class.getSimpleName());
    }

    public LemmatizedMetadataOld(String propertyName) {
        this();
        this.scope = new ColumnMetadata(propertyName);
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {
//        List<LemmatizedMetadataOld> matchedInRepo = metadataRepository.getMetadataPool().stream()
//                .filter(metadata -> metadata instanceof LemmatizedMetadataOld)
//                .map(metadata -> (LemmatizedMetadataOld) metadata)
//                .filter(metadata -> metadata.equals(this))
//                .collect(Collectors.toList());
//
//        if (matchedInRepo.size() == 0) {
//            throw new MetadataNotFoundException(String.format("MetadataOld %s not found in the repository.", this.toString()));
//        } else if (matchedInRepo.size() > 1) {
//            throw new DuplicateMetadataException(String.format("MetadataOld %s has multiple data type for property: %s",
//                    this.getClass().getSimpleName(), this.scope.getName()));
//        } else {
//            LemmatizedMetadataOld metadataInRepo = matchedInRepo.get(0);
//            if (!this.equalsByValue(metadataInRepo)) {
//                // value of this metadata does not match that in the repository.
//                throw new MetadataNotMatchException(String.format("MetadataOld value does not match that in the repository."));
//            }
//        }
    }

    @Override
    public boolean equalsByValue(MetadataOld metadata) {
        return metadata instanceof LemmatizedMetadataOld;
    }
}
