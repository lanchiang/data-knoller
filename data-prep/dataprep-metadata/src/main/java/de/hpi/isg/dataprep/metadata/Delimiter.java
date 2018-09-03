package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.object.Metadata;
import de.hpi.isg.dataprep.model.target.object.OperatedObject;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class Delimiter extends Metadata {

    private final String name = "delimiter";

    private OperatedObject target;

    private String delimiter;

    public Delimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public String getTargetName() {
        return null;
    }

    public String getDelimiter() {
        return delimiter;
    }

    @Override
    public String getName() {
        return name;
    }
}
