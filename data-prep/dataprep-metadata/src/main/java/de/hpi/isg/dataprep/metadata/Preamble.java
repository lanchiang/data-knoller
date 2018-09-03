package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.object.Metadata;

/**
 * A {@link Preamble} is the portion of data file that indicates non-content information. Such as copyright, data using instruction...
 *
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class Preamble extends Metadata {

    private final String name = "preamble";

    // A string may not be a good model for preamble. What is the best way to model all preamble information?
    private String preamble;

    public Preamble(String preamble) {
        this.preamble = preamble;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public String getTargetName() {
        return null;
    }

    public String getPreamble() {
        return preamble;
    }

    @Override
    public String getName() {
        return name;
    }
}
