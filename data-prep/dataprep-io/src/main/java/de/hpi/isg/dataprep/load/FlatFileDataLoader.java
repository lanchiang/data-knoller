package de.hpi.isg.dataprep.load;

import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Lan Jiang
 * @since 2018/9/6
 */
public class FlatFileDataLoader extends SparkDataLoader {

    public FlatFileDataLoader(FileLoadDialect dialect) {
        this.dialect = dialect;
        this.getDialect().setDataSetName("Default dataset name");
    }

    @Override
    public DataContext load() {
        DataFrameReader dataFrameReader = super.createDataFrameReader();
        dataFrame = dataFrameReader.csv(dialect.getUrl());
        return new DataContext(dataFrame, dialect);
    }

    /**
     * If delimiter is not specified, try to infer it.
     */
    private void inferDelimiter() {
    }
}
