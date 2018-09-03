package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.DuplicateMetadataException;
import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.exceptions.MetadataNotMatchException;
import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.object.Metadata;
import de.hpi.isg.dataprep.model.target.object.OperatedObject;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 2018/8/27
 */
public class EndRecordCharacters extends Metadata {

    private final String name = "end-record-characters";

    private OperatedObject target;

    private String endRecordCharacters;

    public EndRecordCharacters(String endRecordCharacters) {
        this.endRecordCharacters = endRecordCharacters;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {
        List<EndRecordCharacters> matchedInRepo = metadataRepository.getMetadataPool().stream()
                .filter(metadata -> metadata instanceof EndRecordCharacters)
                .map(metadata -> (EndRecordCharacters) metadata)
                .filter(metadata -> metadata.getEndRecordCharacters().equals(this.endRecordCharacters))
                .collect(Collectors.toList());

        if (matchedInRepo.size() == 0) {
            // no end record representation discovered.
            throw new MetadataNotFoundException(String.format("Metadata %s not found in the repository", this.getClass().getSimpleName()));
        } else if (matchedInRepo.size() > 1) {
            throw new DuplicateMetadataException(String.format("Multiple pieces of metadata %s found in the repository.",
                    this.getClass().getSimpleName()));
        } else {
            EndRecordCharacters metadataInRepo = matchedInRepo.get(0);
            if (!metadataInRepo.getEndRecordCharacters().equals(this.endRecordCharacters)) {
                throw new MetadataNotMatchException(String.format("Metadata value does not match that in the repository."));
            }
        }
    }

    @Override
    public String getTargetName() {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    public String getEndRecordCharacters() {
        return endRecordCharacters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EndRecordCharacters that = (EndRecordCharacters) o;
        return Objects.equals(endRecordCharacters, that.endRecordCharacters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endRecordCharacters);
    }

    @Override
    public String toString() {
        return "EndRecordCharacters{" +
                "endRecordCharacters='" + endRecordCharacters + '\'' +
                '}';
    }
}
