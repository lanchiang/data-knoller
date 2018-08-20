package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.implementation.abstracts.RenamePropertyImpl;
import de.hpi.isg.dataprep.model.metadata.RenamePropertyMetadata;
import de.hpi.isg.dataprep.model.target.error.ErrorLog;
import de.hpi.isg.dataprep.model.target.error.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 2018/8/17
 */
public class RenameProperty extends Preparator {

    private String propertyName;
    private String newPropertyName;

    public RenameProperty(RenamePropertyImpl impl) {
        this.impl = impl;
        prerequisites = new RenamePropertyMetadata();
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

    public String getNewPropertyName() {
        return newPropertyName;
    }

    public void setNewPropertyName(String newPropertyName) {
        this.newPropertyName = newPropertyName;
    }
}
