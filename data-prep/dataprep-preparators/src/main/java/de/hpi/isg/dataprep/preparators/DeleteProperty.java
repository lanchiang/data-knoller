package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.implementation.DeletePropertyImpl;
import de.hpi.isg.dataprep.model.metadata.DeletePropertyMetadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;

/**
 * @author Lan Jiang
 * @since 2018/8/20
 */
public class DeleteProperty extends Preparator {

    private String propertyName;

    public DeleteProperty(DeletePropertyImpl impl) {
        this.impl = impl;
        prerequisites = DeletePropertyMetadata.getInstance();
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
}
