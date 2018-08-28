package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.Metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class ZipcodeType extends Metadata {

    private final String name = "zip-code-type";

    private String propertyName;
    private ZipcodeType zipcodeType;

    public ZipcodeType(String propertyName, ZipcodeType zipcodeType) {
        this.propertyName = propertyName;
        this.zipcodeType = zipcodeType;
    }

    @Override
    public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException {

    }

    public String getPropertyName() {
        return propertyName;
    }

    public ZipcodeType getZipcodeType() {
        return zipcodeType;
    }

    @Override
    public String getName() {
        return name;
    }
}
