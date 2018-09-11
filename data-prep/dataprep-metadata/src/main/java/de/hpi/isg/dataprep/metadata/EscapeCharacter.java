package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class EscapeCharacter extends Metadata {

    private final String name = "escape-characters";

    private String escapeCharacter;

    public EscapeCharacter(String escapeCharacter) {
        this.escapeCharacter = escapeCharacter;
    }

    public EscapeCharacter(MetadataScope scope, String escapeCharacter) {
        this.scope = scope;
        this.escapeCharacter = escapeCharacter;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return this.equals(metadata);
    }

    public String getEscapeCharacter() {
        return escapeCharacter;
    }

    @Override
    public String getName() {
        return scope.getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EscapeCharacter that = (EscapeCharacter) o;
        return Objects.equals(scope, that.scope) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {

        return Objects.hash(scope, name);
    }
}
