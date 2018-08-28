package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.DuplicateMetadataException;
import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.exceptions.MetadataNotMatchException;
import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.util.DataType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 2018/8/25
 */
public class PropertyDataType extends Metadata {

    private final String name = "property-data-type";

    private String propertyName;
    private DataType.PropertyType propertyDataType;

    public PropertyDataType(String propertyName, DataType.PropertyType propertyDataType) {
        this.propertyName = propertyName;
        this.propertyDataType = propertyDataType;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public DataType.PropertyType getPropertyDataType() {
        return propertyDataType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void checkMetadata(MetadataRepository<?> metadataRepository) throws RuntimeMetadataException {
        List<PropertyDataType> matchedInRepo = metadataRepository.getMetadataPool().stream()
                .filter(metadata -> metadata instanceof PropertyDataType)
                .map(metadata -> (PropertyDataType) metadata)
                .filter(metadata -> metadata.getPropertyName().equals(this.propertyName))
                .collect(Collectors.toList());

        if (matchedInRepo.size() == 0) {
            throw new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", this.toString()));
        } else if (matchedInRepo.size() > 1) {
            throw new DuplicateMetadataException(String.format("Metadata %s has multiple data type for property: %s", this.getClass().getSimpleName(), this.propertyName));
        } else {
            PropertyDataType metadataInRepo = matchedInRepo.get(0);
            if (!this.propertyDataType.equals(metadataInRepo.getPropertyDataType())) {
                // value of this metadata does not match that in the repository.
                throw new MetadataNotMatchException(String.format("Metadata value does not match that in the repository."));
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PropertyDataType that = (PropertyDataType) o;
        return Objects.equals(propertyName, that.propertyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(propertyName);
    }

    @Override
    public String toString() {
        return "PropertyDataType{" +
                "propertyName='" + propertyName + '\'' +
                ", propertyDataType=" + propertyDataType +
                '}';
    }
}
