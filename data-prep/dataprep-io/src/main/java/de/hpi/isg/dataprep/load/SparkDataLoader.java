package de.hpi.isg.dataprep.load;

import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.metadata.Delimiter;
import de.hpi.isg.dataprep.metadata.QuoteCharacter;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author Lan Jiang
 * @since 2018/9/6
 */
abstract public class SparkDataLoader {

    protected String encoding;

    protected FileLoadDialect dialect;
    protected Dataset<Row> dataFrame;

    public SparkDataLoader() {
    }

    protected DataFrameReader createDataFrameReader() {
        DataFrameReader dataFrameReader = SparkSession.builder()
                .appName(dialect.getSparkAppName())
                .master(dialect.getMasterUrl()).getOrCreate().read();

        dataFrameReader
                .option("sep", dialect.getDelimiter())
                .option("quote", dialect.getQuoteChar())
                .option("escape", dialect.getEscapeChar())
                .option("header", dialect.getHasHeader())
                .option("encoding", dialect.getEncoding())
                .option("inferSchema", dialect.getInferSchema());

        return dataFrameReader;
    }

    abstract public DataContext load();

    public FileLoadDialect getDialect() {
        return dialect;
    }

    public Dataset<Row> getDataFrame() {
        return dataFrame;
    }
}
