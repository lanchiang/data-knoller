package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.implementation.DeletePropertyImpl;
import de.hpi.isg.dataprep.model.prepmetadata.DeletePropertyMetadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;

/**
 * @author Lan Jiang
 * @since 2018/8/20
 */
public class DeleteProperty extends Preparator {

    private String propertyName;

    public DeleteProperty(PreparatorImpl impl) {
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
}
