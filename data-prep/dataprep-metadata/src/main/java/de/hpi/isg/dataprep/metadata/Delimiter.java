package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.Metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class Delimiter extends Metadata {

    private final String name = "delimiter";

    private String delimiter;

    public Delimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException {

    }

    public String getDelimiter() {
        return delimiter;
    }

    @Override
    public String getName() {
        return name;
    }
}
