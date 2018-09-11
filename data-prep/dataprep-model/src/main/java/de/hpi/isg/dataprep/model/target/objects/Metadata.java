package de.hpi.isg.dataprep.model.target.objects;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.Target;
import de.hpi.isg.dataprep.util.Nameable;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The super class of all {@link Metadata}.
 *
 * @author Lan Jiang
 * @since 2018/8/25
 */
abstract public class Metadata extends Target implements Serializable, Nameable {

    private static final long serialVersionUID = 1863322602595412693L;

    protected MetadataScope scope;

    /**
     * Which metadata repository this metadata belongs to.
     */
    transient protected MetadataRepository belongs;

    /**
     * Check whether this metadata exists in the belongs metadata repository.
     * @return
     */
//    abstract public boolean isExist();

    /**
     * Check whether the value of this metadata reconciles with that in the metadata repository.
     *
     * @param metadataRepository represents the {@link MetadataRepository} of this {@link de.hpi.isg.dataprep.model.target.system.AbstractPipeline}.
     * @throws Exception
     */
    abstract public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException;

//    public void updateMetadata() {
//        if (!isExist()) {
//            belongs.getMetadataPool().add(this);
//        } else {
//            // If the metadata is in the repository, keep all the other except the metadata sharing the same name with the one in parameter.
//            Set<Metadata> tmpMetadataPool;
//            if (scope instanceof Property) {
//                tmpMetadataPool = belongs.getMetadataPool().stream()
//                        .filter(metadata -> metadata.getName().equals(this.getName())).collect(Collectors.toSet());
//            } else {
//                // scope is instance of DataSet
//                tmpMetadataPool = belongs.getMetadataPool().stream()
//                        .filter(metadata -> metadata.equals(this)).collect(Collectors.toSet());
//            }
//            tmpMetadataPool.add(this);
//            belongs.setMetadataPool(tmpMetadataPool);
//        }
//    }

    abstract public boolean equalsByValue(Metadata metadata);

    public MetadataRepository getBelongs() {
        return belongs;
    }

    public void setBelongs(MetadataRepository belongs) {
        this.belongs = belongs;
    }

    public MetadataScope getScope() {
        return scope;
    }

    @Override
    public String getName() {
        return scope.getName();
    }
}
