package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class AddressFormat extends Metadata {

    private final String name = "address-format";

    private String propertyName;
    private AddressFormat addressFormat;

    public AddressFormat(String propertyName, AddressFormat addressFormat) {
        this.propertyName = propertyName;
        this.addressFormat = addressFormat;
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

    public AddressFormat getAddressFormat() {
        return addressFormat;
    }

    @Override
    public String getName() {
        return null;
    }
}
