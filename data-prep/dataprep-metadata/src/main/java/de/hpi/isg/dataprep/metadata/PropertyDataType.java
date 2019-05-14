package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.exceptions.RuntimeMetadataException;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.MetadataOld;
import de.hpi.isg.dataprep.model.target.objects.ColumnMetadata;
import de.hpi.isg.dataprep.util.DataType;

/**
 * @author Lan Jiang
 * @since 2018/8/25
 */
public class PropertyDataType extends MetadataOld {

    private DataType.PropertyType propertyDataType;

    private PropertyDataType() {
        super(de.hpi.isg.dataprep.PropertyDataType.class.getSimpleName());
    }

    public PropertyDataType(String propertyName, DataType.PropertyType propertyDataType) {
        this();
        this.scope = new ColumnMetadata(propertyName);
        this.propertyDataType = propertyDataType;
    }

    public DataType.PropertyType getPropertyDataType() {
        return propertyDataType;
    }

    @Override
    public void checkMetadata(MetadataRepository metadataRepository) throws RuntimeMetadataException {
//        List<de.hpi.isg.dataprep.PropertyDataType> matchedInRepo = metadataRepository.getMetadataPool().stream()
//                .filter(metadata -> metadata instanceof de.hpi.isg.dataprep.PropertyDataType)
//                .map(metadata -> (de.hpi.isg.dataprep.PropertyDataType) metadata)
//                .filter(metadata -> metadata.equals(this))
//                .collect(Collectors.toList());
//
//        if (matchedInRepo.size() == 0) {
//            throw new MetadataNotFoundException(String.format("MetadataOld %s not found in the repository.", this.toString()));
//        } else if (matchedInRepo.size() > 1) {
//            throw new DuplicateMetadataException(String.format("MetadataOld %s has multiple data type for property: %s",
//                    this.getClass().getSimpleName(), this.scope.getName()));
//        } else {
//            de.hpi.isg.dataprep.PropertyDataType metadataInRepo = matchedInRepo.get(0);
//            if (!this.equalsByValue(metadataInRepo)) {
//                // value of this metadata does not match that in the repository.
//                throw new MetadataNotMatchException(String.format("MetadataOld value does not match that in the repository."));
//            }
//        }
    }

    @Override
    public boolean equalsByValue(MetadataOld metadata) {
//        if(metadata instanceof de.hpi.isg.dataprep.PropertyDataType)
//            return propertyDataType.equals(((de.hpi.isg.dataprep.PropertyDataType) metadata).getPropertyDataType());
        return false;
    }

    @Override
    public String toString() {
        return "PropertyDataType{" +
                "propertyName='" + scope.getName() + '\'' +
                ", propertyDataType=" + propertyDataType +
                '}';
    }
}
