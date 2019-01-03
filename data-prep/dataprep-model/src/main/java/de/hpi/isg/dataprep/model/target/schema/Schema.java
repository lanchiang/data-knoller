package de.hpi.isg.dataprep.model.target.schema;

import org.apache.spark.sql.types.StructField;

/**
 * This class represents the schema of the processed data.
 *
 * @author: lan.jiang
 * @since: 12/17/18
 */
public class Schema {

    private StructField[] schema;

    public Schema(StructField[] schema) {
        this.schema = schema;
    }

    public StructField[] getSchema() {
        return schema;
    }
}
