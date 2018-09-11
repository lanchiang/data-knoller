package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;

/**
 * @author Lan Jiang
 * @since 2018/8/27
 */
public class NullCharacters extends Metadata {

    private final String name = "null-characters";

    private MetadataScope target;

    private String nullCharacters;

    public NullCharacters(String nullCharacters) {
        this.nullCharacters = nullCharacters;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public String getTargetName() {
        return null;
    }

    @Override
    public String getName() {
        return name;
    }
}
