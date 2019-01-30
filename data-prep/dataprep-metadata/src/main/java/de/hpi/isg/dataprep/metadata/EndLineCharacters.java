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

    private String endLineCharacters;

    public EndLineCharacters(String endLineCharacters) {
        this();
        this.endLineCharacters = endLineCharacters;
    }

    private EndLineCharacters() {
        super(EndLineCharacters.class.getSimpleName());
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return this.equals(metadata);
    }

    @Override
    public String toString() {
        return "EndLineCharacters{" +
                "endLineCharacters='" + endLineCharacters + '\'' +
                '}';
    }
}
