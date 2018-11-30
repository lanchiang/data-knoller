package de.hpi.isg.dataprep.context;

import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Lan Jiang
 * @since 2018/9/11
 */
public class DataContext {

    private Dataset<Row> dataFrame;
    private FileLoadDialect dialect;

    public DataContext(Dataset<Row> dataFrame, FileLoadDialect dialect) {
        this.dataFrame = dataFrame;
        this.dialect = dialect;
    }

    public Dataset<Row> getDataFrame() {
        return dataFrame;
    }

    public FileLoadDialect getDialect() {
        return dialect;
    }

}
