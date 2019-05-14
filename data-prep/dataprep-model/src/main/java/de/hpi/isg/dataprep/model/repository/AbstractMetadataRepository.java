package de.hpi.isg.dataprep.model.repository;

import de.hpi.isg.dataprep.Metadata;

import java.util.Collection;
import java.util.List;

/**
 * This abstract class provides the APIs of metadata repository classes.
 *
 * @author Lan Jiang
 * @since 2019-05-14
 */
public interface AbstractMetadataRepository {

    /**
     * Update the metadata pool with the given metadata. If the pool does not contain this metadata, add a new one. If the pool contains it, then first delete the old one, and
     * the add the given metadata. Note that {@link Metadata} is compared only by its name and its scope, except for its value.
     *
     * @param metadata is the metadata to update the metadata pool.
     */
    void update(Metadata metadata);

    /**
     * Add a metadata to the metadata repository.
     *
     * @param metadata is the metadata to be added.
     */
    void add(Metadata metadata);

    /**
     * Remove the given metadata from the repository.
     *
     * @param metadata is the metadata to be removed.
     */
    void remove(Metadata metadata);

    // Todo: this should be removed when all the usage are replaced by the cognominal function in the metadata engine.
    @Deprecated
    void update(Collection<Metadata> metadataCollection);

    /**
     * Return true if the repository contains the metadata with the same name regardless of the values of the metadata.
     *
     * @param metadata the metadata to be checked.
     * @return true if the repository contains the metadata with the same name regardless of the values of the metadata.
     */
    boolean isExist(Metadata metadata);

    /**
     * Return <code>true</code> if the repository contains the metadata given by the parameter.
     *
     * @param metadata the metadata to be checked.
     * @return <code>true</code> if the repository contains the metadata.
     */
    boolean contains(Metadata metadata);

    /**
     * Get the metadata with the same signature from the repository. Evaluating same signatures inspects whether scope and name are the same.
     *
     * @param metadata
     * @return
     */
    Metadata getMetadata(Metadata metadata);

    /**
     * Get the metadata of particular type from the repository.
     *
     * @param clazz the class instance of the particular metadata type.
     * @param <T> the particular metadata type.
     * @return a list of metadata in the repository of the given type.
     */
    <T extends Metadata> List<T> getMetadataOfType(Class<T> clazz);

    /**
     * Clear the metadata repository.
     */
    void clear();
}
