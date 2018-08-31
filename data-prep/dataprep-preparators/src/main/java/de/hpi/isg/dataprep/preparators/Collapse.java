package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.implementation.CollapseImpl;
import de.hpi.isg.dataprep.metadata.PropertyDataType;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.util.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * This preparator remove all but keep one white space of all values in a target.
 *
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class Collapse extends Preparator {

    // but actually a target can be a property, or a dataset.
    private String propertyName;

    public Collapse(PreparatorImpl impl) {
        this.impl = impl;
    }

    @Override
    public void buildMetadataSetup() throws ParameterNotSpecifiedException {
        List<Metadata> prerequisites = new ArrayList<>();
        List<Metadata> tochanges = new ArrayList<>();

        if (propertyName == null) {
            throw new ParameterNotSpecifiedException(String.format("%s not specified", propertyName));
        }
        // Collapse can only be applied on String data type
        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING));

        this.prerequisite.addAll(prerequisites);
        this.toChange.addAll(tochanges);
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }
}
