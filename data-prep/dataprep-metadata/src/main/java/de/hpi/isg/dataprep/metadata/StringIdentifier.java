package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.object.Metadata;

/**
 * A pairwise string identifier wraps the position of real data content in a data file.
 *
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class StringIdentifier extends Metadata {

    private final String name = "string-identifier";

    private String stringIdentifier;

    public StringIdentifier(String stringIdentifier) {
        this.stringIdentifier = stringIdentifier;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public String getTargetName() {
        return null;
    }

    public String getStringIdentifier() {
        return stringIdentifier;
    }

    @Override
    public String getName() {
        return name;
    }
}
