package de.hpi.isg.dataprep.model.target.data;

import de.hpi.isg.dataprep.model.target.Target;

/**
 * @author Lan Jiang
 * @since 2018/12/16
 */
public class Column extends Target {

    private int columnId;
    private String columnName;

    public Column(int columnId, String columnName) {
        this.columnId = columnId;
        this.columnName = columnName;
    }

    public Column(String columnName) {
        this.columnName = columnName;
        this.columnId = createColumnId();
    }

    private int createColumnId() {
        return 0;
    }

    public int getColumnId() {
        return columnId;
    }

    public String getColumnName() {
        return columnName;
    }
}
