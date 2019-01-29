package de.hpi.isg.dataprep.load;

import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Set;

/**
 * @author Lan Jiang
 * @since 2018/9/6
 */
abstract public class SparkDataLoader {

    protected String encoding;

    protected FileLoadDialect dialect;
    protected Dataset<Row> dataFrame;

    protected List<Transform> transforms;

    protected Set<Metadata> targetMetadata;
    protected SchemaMapping schemaMapping;

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

    abstract protected SchemaMapping createSchemaMapping();

    public FileLoadDialect getDialect() {
        return dialect;
    }

    public Dataset<Row> getDataFrame() {
        return dataFrame;
    }
}
