package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.cases.BooleanType;
import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.Metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class PropertyBooleanType extends Metadata {

    private final String name = "property-boolean-type";

    private String propertyName;
    private BooleanType booleanType;

    public PropertyBooleanType(String propertyName, BooleanType booleanType) {
        this.propertyName = propertyName;
        this.booleanType = booleanType;
    }

    @Override
    public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException {

    }

    public String getPropertyName() {
        return propertyName;
    }

    public BooleanType getBooleanType() {
        return booleanType;
    }

    @Override
    public String getName() {
        return null;
    }
}
