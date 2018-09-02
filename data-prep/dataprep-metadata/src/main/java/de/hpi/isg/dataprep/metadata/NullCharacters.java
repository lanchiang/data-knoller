package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.object.Metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/27
 */
public class NullCharacters extends Metadata {

    private final String name = "null-characters";

    private String nullCharacters;

    public NullCharacters(String nullCharacters) {
        this.nullCharacters = nullCharacters;
    }

    @Override
    public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public String getName() {
        return name;
    }
}
