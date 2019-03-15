package de.hpi.isg.dataprep.model.target.objects;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/9/2
 */
public class TableMetadata extends MetadataScope {

    private String tableName;

    public TableMetadata(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableMetadata tableMetadata = (TableMetadata) o;
        return Objects.equals(tableName, tableMetadata.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName);
    }
}
