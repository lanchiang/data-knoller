package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.implementation.AddPropertyImpl;
import de.hpi.isg.dataprep.model.metadata.AddPropertyMetadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.PropertyDataType;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/8/20
 */
public class AddProperty extends Preparator {

    private String targetPropertyName;
    private PropertyDataType.PropertyType targetPropertyDataType;
    private int positionInSchema;
    private Object defaultValue;

    public AddProperty(AddPropertyImpl impl) {
        this.impl = impl;
        prerequisites = AddPropertyMetadata.getInstance();
    }

    @Override
    protected void recordProvenance() {

    }

    public String getTargetPropertyName() {
        return targetPropertyName;
    }

    public void setTargetPropertyName(String targetPropertyName) {
        this.targetPropertyName = targetPropertyName;
    }

    public PropertyDataType.PropertyType getTargetPropertyDataType() {
        return targetPropertyDataType;
    }

    public void setTargetPropertyDataType(PropertyDataType.PropertyType targetPropertyDataType) {
        this.targetPropertyDataType = targetPropertyDataType;
    }

    public int getPositionInSchema() {
        return positionInSchema;
    }

    public void setPositionInSchema(int positionInSchema) {
        this.positionInSchema = positionInSchema;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }
}
