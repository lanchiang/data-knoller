package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/8/27
 */
public class EndLineCharacters extends Metadata {

    private final String name;

    private String endLineCharacters;

    public EndLineCharacters(String endLineCharacters) {
        this();
        this.endLineCharacters = endLineCharacters;
    }

    public EndLineCharacters() {
        name = "end-line-characters";
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return this.equals(metadata);
    }

    @Override
    public String getName() {
        return scope.getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EndLineCharacters)) return false;
        EndLineCharacters that = (EndLineCharacters) o;
        return Objects.equals(scope, that.scope) &&
                Objects.equals(endLineCharacters, that.endLineCharacters);
    }

    @Override
    public int hashCode() {

        return Objects.hash(scope, endLineCharacters);
    }

    @Override
    public String toString() {
        return "EndLineCharacters{" +
                "endLineCharacters='" + endLineCharacters + '\'' +
                '}';
    }
}
