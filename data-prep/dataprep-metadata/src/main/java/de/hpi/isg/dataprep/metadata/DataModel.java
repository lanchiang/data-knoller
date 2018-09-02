package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.object.Metadata;

/**
 * The original data model used to represent this dataset, although the data is represented as relational data inside this system.
 *
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class DataModel extends Metadata {

    private final String name = "data-model";

    // find something better than a String.
    private String dataModel;

    public DataModel(String dataModel) {
        this.dataModel = dataModel;
    }

    @Override
    public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException {

    }

    public String getDataModel() {
        return dataModel;
    }

    @Override
    public String getName() {
        return name;
    }
}
