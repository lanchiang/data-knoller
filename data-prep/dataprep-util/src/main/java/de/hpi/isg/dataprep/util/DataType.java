package de.hpi.isg.dataprep.util;

import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;

import java.io.Serializable;

/**
 * @author Lan Jiang
 * @since 2018/8/16
 */
public class DataType implements Serializable {

    private static final long serialVersionUID = -3537107727887529254L;

    public enum PropertyType {
        STRING,
        INTEGER,
        DOUBLE,
        DATE,
        DATETIME
    }

    public static PropertyType getTypeFromSparkType(org.apache.spark.sql.types.DataType dataType) {
        PropertyType innerType = null;
        if (dataType instanceof StringType) {
            innerType = PropertyType.STRING;
        } else if (dataType instanceof IntegerType) {
            innerType = PropertyType.INTEGER;
        } else if (dataType instanceof DoubleType) {
            innerType = PropertyType.DOUBLE;
        } else if (dataType instanceof DateType) {
            innerType = PropertyType.DATE;
        } else {
            // pass
        }
        return innerType;
    }
}
