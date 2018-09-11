package de.hpi.isg.dataprep.model.target.objects;

import java.util.Objects;

/**
 * The instance of this class represents a property in a dataset. Depending on the data model used, it may be characterised as different entity.
 * Such as a column in a relational data model, or an attribute in an XML file.
 *
 * @author Lan Jiang
 * @since 2018/9/2
 */
public class Property extends MetadataScope {

    String propertyName;

    public Property(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public String getName() {
        return propertyName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Property property = (Property) o;
        return Objects.equals(propertyName, property.propertyName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(propertyName);
    }
}
