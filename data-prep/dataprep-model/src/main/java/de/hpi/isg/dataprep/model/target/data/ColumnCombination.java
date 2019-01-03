package de.hpi.isg.dataprep.model.target.data;

import de.hpi.isg.dataprep.model.target.Target;

import java.util.Arrays;

/**
 * @author Lan Jiang
 * @since 2018/12/16
 */
public class ColumnCombination extends Target {

    private Column[] columns;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnCombination that = (ColumnCombination) o;
        return Arrays.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(columns);
    }
}
