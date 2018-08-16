package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.implementation.abstracts.ChangePropertyDataTypeImpl;
import de.hpi.isg.dataprep.model.metadata.ChangePropertyDataTypeMetadata;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.DatePattern;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Lan Jiang
 * @since 2018/8/7
 */
public class ChangePropertyDataType extends Preparator {

    public enum PropertyType {
        STRING,
        INTEGER,
        DOUBLE,
        DATE,
        DATETIME
    }

    private String propertyName;
    private PropertyType targetType;
    private DatePattern.DatePatternEnum sourceDatePattern;
    private DatePattern.DatePatternEnum targetDatePattern;

    private ChangePropertyDataTypeImpl impl;

    public ChangePropertyDataType(String propertyName, PropertyType targetType) {
        this.propertyName = propertyName;
        this.targetType = targetType;
    }

    public ChangePropertyDataType(String propertyName, PropertyType sourceType, PropertyType targetType) {
        this(propertyName, targetType);
    }

    public ChangePropertyDataType(ChangePropertyDataTypeImpl impl) {
        this.impl = impl;
    }

    @Override
    protected boolean checkMetadata() {
        /**
         * check for each metadata whether valid.
         * Stores invalid ones.
         * If all prerequisites are met, read and store all these metadata, used in preparator execution.
         */
        prerequisites = new ChangePropertyDataTypeMetadata();
        Map<String, String> validMetadata = new HashMap<>();
        for (String metadata : prerequisites.getPrerequisites()) {
            if (false) {
                // this metadata is not satisfied, add it to the invalid metadata set.
                this.getInvalid().add(new Metadata(metadata));
                return false;
            }
            validMetadata.putIfAbsent(metadata, null);
        }
        this.metadataValue = validMetadata;
        return true;
    }

    @Override
    protected void executePreparator() throws Exception {
        impl.execute(this);
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

    public PropertyType getTargetType() {
        return targetType;
    }

    public void setTargetType(PropertyType targetType) {
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
