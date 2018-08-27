package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.implementation.RenamePropertyImpl;
import de.hpi.isg.dataprep.model.prepmetadata.RenamePropertyMetadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;

/**
 * @author Lan Jiang
 * @since 2018/8/17
 */
public class RenameProperty extends Preparator {

    private String propertyName;
    private String newPropertyName;

    public RenameProperty(RenamePropertyImpl impl) {
        this.impl = impl;
    }

    @Override
    public void buildMetadataSetup() throws ParameterNotSpecifiedException {

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
