package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.implementation.ChangePropertyDataTypeImpl;
import de.hpi.isg.dataprep.model.metadata.ChangePropertyDataTypeMetadata;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.DatePattern;
import de.hpi.isg.dataprep.util.PropertyDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Lan Jiang
 * @since 2018/8/7
 */
public class ChangePropertyDataType extends Preparator {

    private String propertyName;
    private PropertyDataType.PropertyType targetType;
    private DatePattern.DatePatternEnum sourceDatePattern;
    private DatePattern.DatePatternEnum targetDatePattern;

    public ChangePropertyDataType(String propertyName, PropertyDataType.PropertyType targetType) {
        this.propertyName = propertyName;
        this.targetType = targetType;
    }

    public ChangePropertyDataType(String propertyName, PropertyDataType.PropertyType sourceType, PropertyDataType.PropertyType targetType) {
        this(propertyName, targetType);
    }


    public ChangePropertyDataType(ChangePropertyDataTypeImpl impl) {
        this.impl = impl;
        prerequisites = new ChangePropertyDataTypeMetadata();
    }

    @Override
    protected void recordProvenance() {

    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public PropertyDataType.PropertyType getTargetType() {
        return targetType;
    }

    public void setTargetType(PropertyDataType.PropertyType targetType) {
        this.targetType = targetType;
    }

    public DatePattern.DatePatternEnum getTargetDatePattern() {
        return targetDatePattern;
    }

    public void setTargetDatePattern(DatePattern.DatePatternEnum targetDatePattern) {
        this.targetDatePattern = targetDatePattern;
    }

    public DatePattern.DatePatternEnum getSourceDatePattern() {
        return sourceDatePattern;
    }

    public void setSourceDatePattern(DatePattern.DatePatternEnum sourceDatePattern) {
        this.sourceDatePattern = sourceDatePattern;
    }
}
