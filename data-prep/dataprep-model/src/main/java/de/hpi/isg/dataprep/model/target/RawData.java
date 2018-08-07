package de.hpi.isg.dataprep.model.target;


import de.hpi.isg.dataprep.DatasetUtil;
import de.hpi.isg.dataprep.util.DatasetConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Raw data is the file level representation of the dataset.
 *
 * @author Lan Jiang
 * @since 2018/8/3
 */
public class RawData {

    private Dataset<Row> lines;

    public RawData(DatasetConfig datasetConfig) {
        lines = DatasetUtil.createDataset(datasetConfig);
    }
}
