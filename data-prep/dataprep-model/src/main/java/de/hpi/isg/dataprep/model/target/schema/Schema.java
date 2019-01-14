package de.hpi.isg.dataprep.model.target.schema;

/**
 * This class represents the schema of the processed data.
 *
 * @author: lan.jiang
 * @since: 12/17/18
 */
public class Schema {

    private Attribute[] schema;

    public Schema(Attribute[] schema) {
        this.schema = schema;
    }

    public Attribute[] getSchema() {
        return schema;
    }
}
