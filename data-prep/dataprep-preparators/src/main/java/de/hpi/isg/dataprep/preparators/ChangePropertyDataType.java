package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.implementation.ChangePropertyDataTypeImpl;
import de.hpi.isg.dataprep.metadata.PropertyDataType;
import de.hpi.isg.dataprep.metadata.PropertyDatePattern;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.DatePattern;
import de.hpi.isg.dataprep.util.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/7
 */
public class ChangePropertyDataType extends Preparator {

    private String propertyName;
    private DataType.PropertyType sourceType;
    private DataType.PropertyType targetType;
    private DatePattern.DatePatternEnum sourceDatePattern;
    private DatePattern.DatePatternEnum targetDatePattern;

    public ChangePropertyDataType(ChangePropertyDataTypeImpl impl) {
        this.impl = impl;

    }

    @Override
    public void buildMetadataSetup() throws ParameterNotSpecifiedException {
        // source type parameter was not specified. Use the parameter from metadata repository
        // during compiling time, how to get the up-to-date metadata from the repository?
        List<Metadata> prerequistes = new ArrayList<>();
        List<Metadata> tochanges = new ArrayList<>();

        if (propertyName == null) {
            throw new ParameterNotSpecifiedException("Property name not specified!");
        }
        if (targetType == null) {
            throw new ParameterNotSpecifiedException("Target data type not specified!");
        } else {
            tochanges.add(new PropertyDataType(propertyName, targetType));
        }

        if (sourceType != null) {
            prerequistes.add(new PropertyDataType(propertyName, sourceType));
        } else {
            // use the value in the metadata repository.
        }

        if (targetType.equals(DataType.PropertyType.DATE)) {
            if (sourceDatePattern != null) {
                prerequistes.add(new PropertyDatePattern(propertyName, sourceDatePattern));
            } else {
                // use the value in the metadata repository.
            }

            if (targetDatePattern == null) {
                throw new ParameterNotSpecifiedException("Change to DATE type but target date pattern not specified!");
            } else {
                tochanges.add(new PropertyDatePattern(propertyName, targetDatePattern));
            }
        }

        this.prerequisite.addAll(prerequistes);
        this.toChange.addAll(tochanges);
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public DataType.PropertyType getTargetType() {
        return targetType;
    }

    public void setTargetType(DataType.PropertyType targetType) {
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

    public DataType.PropertyType getSourceType() {
        return sourceType;
    }

    public void setSourceType(DataType.PropertyType sourceType) {
        this.sourceType = sourceType;
    }
}
