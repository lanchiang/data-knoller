package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;

/**
 * This metadata specifies the existence of preambles in a DataFrame.
 *
 * @author Lasse Kohlmeyer
 * @since 2018/11/28
 */
public class PreambleExistence extends Metadata {

    private boolean preambleExistence;

    private PreambleExistence() {
        super(PreambleExistence.class.getSimpleName());
    }

    public PreambleExistence(boolean preambleExistence/*, MetadataScope scope*/) {
        this();
        //this.scope = scope;
        this.preambleExistence = preambleExistence;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return this.equals(metadata);
    }

    public boolean isPreambleExistence() {
        return preambleExistence;
    }
}
