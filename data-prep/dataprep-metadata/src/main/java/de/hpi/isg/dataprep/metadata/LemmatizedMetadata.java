package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.DuplicateMetadataException;
import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.exceptions.MetadataNotMatchException;
import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.ColumnMetadata;
import de.hpi.isg.dataprep.model.target.objects.Metadata;

import java.util.List;
import java.util.stream.Collectors;

public class LemmatizedMetadata extends Metadata {

    public LemmatizedMetadata() {
        super(LemmatizedMetadata.class.getSimpleName());
    }

    public LemmatizedMetadata(String propertyName) {
        this();
        this.scope = new ColumnMetadata(propertyName);
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {
        List<LemmatizedMetadata> matchedInRepo = metadataRepository.getMetadataPool().stream()
                .filter(metadata -> metadata instanceof LemmatizedMetadata)
                .map(metadata -> (LemmatizedMetadata) metadata)
                .filter(metadata -> metadata.equals(this))
                .collect(Collectors.toList());

        if (matchedInRepo.size() == 0) {
            throw new MetadataNotFoundException(String.format("Metadata %s not found in the repository.", this.toString()));
        } else if (matchedInRepo.size() > 1) {
            throw new DuplicateMetadataException(String.format("Metadata %s has multiple data type for property: %s",
                    this.getClass().getSimpleName(), this.scope.getName()));
        } else {
            LemmatizedMetadata metadataInRepo = matchedInRepo.get(0);
            if (!this.equalsByValue(metadataInRepo)) {
                // value of this metadata does not match that in the repository.
                throw new MetadataNotMatchException(String.format("Metadata value does not match that in the repository."));
            }
        }
    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return metadata instanceof LemmatizedMetadata;
    }
}
