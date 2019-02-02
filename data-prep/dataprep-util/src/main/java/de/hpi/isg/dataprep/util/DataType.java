package de.hpi.isg.dataprep.util;

import org.apache.spark.sql.types.*;

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
        DATETIME,
        NONE
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

    public static org.apache.spark.sql.types.DataType getSparkTypeFromInnerType(PropertyType dataType) {
        org.apache.spark.sql.types.DataType javaType = null;
        if (dataType.equals(PropertyType.STRING)) {
            javaType = DataTypes.StringType;
        } else if (dataType.equals(PropertyType.INTEGER)) {
            javaType = DataTypes.IntegerType;
        } else if (dataType.equals(PropertyType.DOUBLE)) {
            javaType = DataTypes.DoubleType;
        } else if (dataType.equals(PropertyType.DATE)) {
            javaType = DataTypes.DateType;
        }
        return javaType;
    }
}
