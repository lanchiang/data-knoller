package de.hpi.isg.dataprep.model.target.objects;

import java.util.Objects;

/**
 * @author Lan Jiang
 * @since 2018/9/2
 */
public class DataSet extends MetadataScope {

    private String dataSetName;

    public DataSet(String dataSetName) {
        this.dataSetName = dataSetName;
    }

    public String getDataSetName() {
        return dataSetName;
    }

    @Override
    public String getName() {
        return dataSetName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSet dataSet = (DataSet) o;
        return Objects.equals(dataSetName, dataSet.dataSetName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(dataSetName);
    }
}
