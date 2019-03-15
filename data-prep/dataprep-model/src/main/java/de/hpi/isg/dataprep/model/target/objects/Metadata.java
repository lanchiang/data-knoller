package de.hpi.isg.dataprep.model.target.objects;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.Target;
import de.hpi.isg.dataprep.util.Nameable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
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
    protected final String name;

    protected Metadata(String name) {
        this.name = name;
    }

    /**
     * Which metadata repository this metadata belongs to.
     */
    transient protected MetadataRepository belongs;

    /**
     * Check whether the value of this metadata conform to those in the metadata repository.
     *
     * @param metadataRepository represents the {@link MetadataRepository} of this {@link de.hpi.isg.dataprep.model.target.system.AbstractPipeline}.
     * @throws Exception
     */
    abstract public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException;

    abstract public boolean equalsByValue(Metadata metadata);

    public void setBelongs(MetadataRepository belongs) {
        this.belongs = belongs;
    }

    public MetadataScope getScope() {
        return scope;
    }

    @Override
    public final String getName() {
        return scope.getName();
    }

    /**
     * Equals if this metadata has the same scope as that.
     * @param o the compared metadata object.
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metadata metadata = (Metadata) o;
        return Objects.equals(scope, metadata.scope) &&
                Objects.equals(name, metadata.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, name);
    }
}
