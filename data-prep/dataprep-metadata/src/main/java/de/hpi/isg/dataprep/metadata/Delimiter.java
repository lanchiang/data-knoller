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
public class Delimiter extends Metadata {

    private String delimiter;

    private Delimiter() {
        super(Delimiter.class.getSimpleName());
    }

    public Delimiter(String delimiter, MetadataScope scope) {
        this();
        this.delimiter = delimiter;
        this.scope = scope;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return this.equals(metadata);
    }

    public String getDelimiter() {
        return delimiter;
    }

}
