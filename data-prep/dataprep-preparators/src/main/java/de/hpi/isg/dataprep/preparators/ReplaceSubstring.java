package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.implementation.ReplaceSubstringImpl;
import de.hpi.isg.dataprep.metadata.PropertyDataType;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.util.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class ReplaceSubstring extends Preparator {

    private String propertyName;
    private String source;
    private String replacement;
    private int firstSome;

    public ReplaceSubstring(PreparatorImpl impl) {
        this.impl = impl;
    }

    @Override
    public void buildMetadataSetup() throws ParameterNotSpecifiedException {
        List<Metadata> prerequisites = new ArrayList<>();
        List<Metadata> tochange = new ArrayList<>();

        if (propertyName == null) {
            throw new ParameterNotSpecifiedException(String.format("Property name not specified"));
        }
        if (source == null) {
            throw new ParameterNotSpecifiedException(String.format("Source sub-string not specified"));
        }
        if (replacement == null) {
            throw new ParameterNotSpecifiedException(String.format("Target sub-string not specified"));
        }
        if (firstSome < 0) {
            throw new IllegalArgumentException(String.format("Cannot replace the first minus sub-strings."));
        }

        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING));

        this.prerequisite.addAll(prerequisites);
        this.toChange.addAll(tochange);
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getReplacement() {
        return replacement;
    }

    public void setReplacement(String replacement) {
        this.replacement = replacement;
    }

    public int getFirstSome() {
        return firstSome;
    }

    public void setFirstSome(int firstSome) {
        this.firstSome = firstSome;
    }
}
