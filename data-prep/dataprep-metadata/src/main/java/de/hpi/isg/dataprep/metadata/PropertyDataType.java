package de.hpi.isg.dataprep.metadata;

import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.util.DataType;

/**
 * @author Lan Jiang
 * @since 2018/8/25
 */
public class PropertyDataType extends Metadata {

    private final String name = "property-data-type";

    private String propertyName;
    private DataType.PropertyType propertyDataType;

    public PropertyDataType() {}

    public PropertyDataType(String propertyName, DataType.PropertyType propertyDataType) {
        this.propertyName = propertyName;
        this.propertyDataType = propertyDataType;
    }

    @Override
    public String getName() {
        return name;
    }
}
