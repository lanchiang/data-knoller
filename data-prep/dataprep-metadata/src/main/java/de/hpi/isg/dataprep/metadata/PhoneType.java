package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class PhoneType extends Metadata {

    private final String name = "phone-type";

    private String propertyName;
    private PhoneType phoneType;

    public PhoneType(String propertyName, PhoneType phoneType) {
        this.propertyName = propertyName;
        this.phoneType = phoneType;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {

    }

    @Override
    public String getTargetName() {
        return propertyName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public PhoneType getPhoneType() {
        return phoneType;
    }

    @Override
    public String getName() {
        return name;
    }
}
