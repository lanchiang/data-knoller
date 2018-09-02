package de.hpi.isg.dataprep.model.target.object;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.Target;
import de.hpi.isg.dataprep.model.target.system.Pipeline;
import de.hpi.isg.dataprep.util.Nameable;

import java.io.Serializable;

/**
 * The super class of all {@link Metadata}.
 *
 * @author Lan Jiang
 * @since 2018/8/25
 */
abstract public class Metadata extends Target implements Serializable, Nameable {

    private static final long serialVersionUID = 1863322602595412693L;

    /**
     * Check whether the value of this metadata reconciles with that in the metadata repository.
     *
     * @param metadataRepository represents the {@link MetadataRepository} of this {@link Pipeline}.
     * @throws Exception
     */
    abstract public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException;

}
