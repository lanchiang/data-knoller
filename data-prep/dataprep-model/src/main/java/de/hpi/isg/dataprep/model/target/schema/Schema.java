package de.hpi.isg.dataprep.model.target.schema;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * This class represents the attributes of the processed data.
 *
 * @author: lan.jiang
 * @since: 12/17/18
 */
public class Schema {

    private List<Attribute> attributes;

    public Schema(List<Attribute> attributes) {
        this.attributes = attributes;
    }

    public Schema(StructType structType) {
        this.attributes = new LinkedList<>();
        for (StructField structField : structType.fields()) {
            this.attributes.add(new Attribute(structField));
        }
    }
    
    public boolean attributeExist(Attribute attribute) {
        return attributes.contains(attribute);
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "attributes=" + attributes.toString() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;
        return Objects.equals(attributes, schema.attributes);
    }

    @Override
    public int hashCode() {

        return Objects.hash(attributes);
    }
}
