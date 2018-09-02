package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.cases.NumberType;
import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.object.Metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class PropertyNumberType extends Metadata {

    private final String name = "property-number-type";

    private String propertyName;
    private NumberType numberType;

    public PropertyNumberType(String propertyName, NumberType numberType) {
        this.propertyName = propertyName;
        this.numberType = numberType;
    }

    @Override
    public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException {

    }

    public String getPropertyName() {
        return propertyName;
    }

    public NumberType getNumberType() {
        return numberType;
    }

    @Override
    public String getName() {
        return name;
    }
}
