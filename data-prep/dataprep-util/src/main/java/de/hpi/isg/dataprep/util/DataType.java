package de.hpi.isg.dataprep.util;

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
}
