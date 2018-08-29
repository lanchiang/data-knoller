package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.implementation.PaddingImpl;
import de.hpi.isg.dataprep.metadata.PropertyDataType;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 *
 *
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class Padding extends Preparator {

    // padding for only this property?
    private String propertyName;
    private int expectedLength;
    private String padder = "0"; // default padder is "0"

    public Padding(PaddingImpl impl) {
        this.impl = impl;
    }

    @Override
    public void buildMetadataSetup() throws ParameterNotSpecifiedException {
        List<Metadata> prerequisites = new ArrayList<>();
        List<Metadata> tochange = new ArrayList<>();

        if (propertyName == null) {
            throw new ParameterNotSpecifiedException(String.format("%s not specified.", propertyName));
        }
        // illegal padding length was input.
        if (expectedLength <= 0) {
            throw new IllegalArgumentException(String.format("Padding length is illegal!"));
        }

        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING));

        // when basic statistics is implemented, one shall be capable of retrieving value length from the metdata repository
        // therefore, this method shall compare the value length as well.

        this.prerequisite.addAll(prerequisites);
        this.toChange.addAll(tochange);
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    public int getExpectedLength() {
        return expectedLength;
    }

    public void setExpectedLength(int expectedLength) {
        this.expectedLength = expectedLength;
    }

    public String getPadder() {
        return padder;
    }

    public void setPadder(String padder) {
        this.padder = padder;
    }
}
