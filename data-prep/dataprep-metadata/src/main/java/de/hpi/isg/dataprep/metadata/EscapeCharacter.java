package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class EscapeCharacter extends Metadata {

    private final String name = "escape-characters";

    private MetadataScope scope;

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
    public String getTargetName() {
        return scope.getName();
    }

    public String getEscapeCharacter() {
        return escapeCharacter;
    }

    @Override
    public String getName() {
        return name;
    }
}
