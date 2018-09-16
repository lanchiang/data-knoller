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

    private String escapeCharacter;

    private EscapeCharacter() {
        super("escape-characters");
    }

    public EscapeCharacter(MetadataScope scope, String escapeCharacter) {
        this();
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
}
