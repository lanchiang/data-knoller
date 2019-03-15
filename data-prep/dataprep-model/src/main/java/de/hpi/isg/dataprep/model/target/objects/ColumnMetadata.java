package de.hpi.isg.dataprep.model.target.objects;

import java.util.Objects;

/**
 * The instance of this class represents a property in a dataset. Depending on the data model used, it may be characterised as different entity.
 * Such as a column in a relational data model, or an attribute in an XML file.
 *
 * @author Lan Jiang
 * @since 2018/9/2
 */
public class ColumnMetadata extends MetadataScope {

    String columnName;

    public ColumnMetadata(String propertyName) {
        this.columnName = propertyName;
    }

    @Override
    public String getName() {
        return columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnMetadata columnMetadata = (ColumnMetadata) o;
        return Objects.equals(columnName, columnMetadata.columnName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(columnName);
    }
}
