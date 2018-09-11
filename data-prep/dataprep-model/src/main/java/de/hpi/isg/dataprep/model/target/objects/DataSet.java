package de.hpi.isg.dataprep.model.target.objects;

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
}
