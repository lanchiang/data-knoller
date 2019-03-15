package de.hpi.isg.dataprep.model.target.schema;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;

import java.io.Serializable;
import java.util.Objects;

/**
 * A wrapper of {@link StructField}. Two such instances equals each other when and only when the name, data type, and nullable of them are the same.
 *
 * @author lan.jiang
 * @since 1/13/19
 */
public class Attribute implements Serializable {

    private static final long serialVersionUID = -7229029676814189794L;

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

    @Override
    public String toString() {
        return "Attribute{" +
                "name='" + name + '\'' +
                ", dataType=" + dataType +
                '}';
    }
}
