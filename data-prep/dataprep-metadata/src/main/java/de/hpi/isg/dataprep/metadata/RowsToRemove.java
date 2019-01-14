package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.MetadataScope;

/**
 * The number of rows of a DataFrame which should be removed
 *
 * @author Lasse Kohlmeyer
 * @since 2018/8/28
 */
public class RowsToRemove extends Metadata {

    private int rowsToRemove;

    private RowsToRemove() {
        super(RowsToRemove.class.getSimpleName());
    }

    public RowsToRemove(int rowsToRemove, MetadataScope scope) {
        this();
        this.scope = scope;
        this.rowsToRemove = rowsToRemove;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public boolean equalsByValue(Metadata metadata) {
        return this.equals(metadata);
    }

    public int getRowsToRemove() {
        return rowsToRemove;
    }
}
