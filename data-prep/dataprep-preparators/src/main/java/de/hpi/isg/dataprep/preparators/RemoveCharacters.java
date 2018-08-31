package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.implementation.RemoveCharactersImpl;
import de.hpi.isg.dataprep.metadata.PropertyDataType;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.util.DataType;
import de.hpi.isg.dataprep.util.RemoveCharactersMode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/30
 */
public class RemoveCharacters extends Preparator {

    private String propertyName;
    private String userSpecifiedCharacters;
    private RemoveCharactersMode mode = null;

    public RemoveCharacters(PreparatorImpl impl) {
        this.impl = impl;
    }

    @Override
    public void buildMetadataSetup() throws ParameterNotSpecifiedException {
        List<Metadata> prerequisites = new ArrayList<>();
        List<Metadata> tochange = new ArrayList<>();

        if (propertyName == null) {
            throw new ParameterNotSpecifiedException(String.format("Property name not specified."));
        } else if (mode == null) {
            throw new ParameterNotSpecifiedException(String.format("Remove character mode not specified."));
        }

        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING));

        if (mode == RemoveCharactersMode.CUSTOM) {
            if (userSpecifiedCharacters == null) {
                throw new ParameterNotSpecifiedException(String.format("Characters must be specified if choosing custom mode."));
            }
        }

        this.prerequisite.addAll(prerequisites);
        this.toChange.addAll(tochange);
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getUserSpecifiedCharacters() {
        return userSpecifiedCharacters;
    }

    public void setUserSpecifiedCharacters(String userSpecifiedCharacters) {
        this.userSpecifiedCharacters = userSpecifiedCharacters;
    }

    public RemoveCharactersMode getMode() {
        return mode;
    }

    public void setMode(RemoveCharactersMode mode) {
        this.mode = mode;
    }
}
