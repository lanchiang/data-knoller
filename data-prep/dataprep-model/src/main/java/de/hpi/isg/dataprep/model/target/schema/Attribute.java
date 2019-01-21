package de.hpi.isg.dataprep.model.target.schema;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;

import java.util.Objects;

/**
 * @author lan.jiang
 * @since 1/13/19
 */
public class Attribute {

    private StructField attribute;

    private String name;
    private DataType dataType;
    private boolean nullable;

    public Attribute(StructField attribute) {
        this.attribute = attribute;

        this.name = attribute.name();
        this.dataType = attribute.dataType();
        this.nullable = attribute.nullable();
    }

    public StructField getAttribute() {
        return attribute;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Attribute attribute = (Attribute) o;
        return nullable == attribute.nullable &&
                Objects.equals(name, attribute.name) &&
                Objects.equals(dataType, attribute.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, nullable);
    }
}
