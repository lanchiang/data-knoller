package de.hpi.isg.dataprep.util.context;

import de.hpi.isg.dataprep.util.dialects.FileLoadDialect;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * The wrapper of {@link org.apache.spark.sql.Dataset} used by a pipeline.
 * 
 * @author Lan Jiang
 * @since 2018/9/10
 */
public class DataContext {
    
    private Dataset<Row> dataset;
    private FileLoadDialect dialect;

    public DataContext(Dataset<Row> dataset, FileLoadDialect dialect) {
        this.dataset = dataset;
        this.dialect = dialect;
    }

    public Dataset<Row> getDataset() {
        return dataset;
    }

    public FileLoadDialect getDialect() {
        return dialect;
    }
}
