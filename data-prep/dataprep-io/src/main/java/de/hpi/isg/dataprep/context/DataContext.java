package de.hpi.isg.dataprep.context;

import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Set;

/**
 * Data context is a package to describe the loaded dataset, as well as the configuration information such as user-specified metadata (delimiter, quote,
 * line terminator...), initial schema mapping (if there is), expected target metadata (if there is).
 *
 * @author Lan Jiang
 * @since 2018/9/11
 */
public class DataContext {

    private Dataset<Row> dataFrame;
    private FileLoadDialect dialect;

    private Set<Metadata> targetMetadata;
    private SchemaMapping schemaMapping;

    public DataContext(Dataset<Row> dataFrame, FileLoadDialect dialect) {
        this(dataFrame, dialect, null, null);
    }

    public DataContext(Dataset<Row> dataFrame, FileLoadDialect dialect,
                       Set<Metadata> targetMetadata, SchemaMapping schemaMapping) {
        this.dataFrame = dataFrame;
        this.dialect = dialect;
        this.targetMetadata = targetMetadata;
        this.schemaMapping = schemaMapping;
    }

    public Dataset<Row> getDataFrame() {
        return dataFrame;
    }

    public FileLoadDialect getDialect() {
        return dialect;
    }

    public Set<Metadata> getTargetMetadata() {
        return targetMetadata;
    }

    public SchemaMapping getSchemaMapping() {
        return schemaMapping;
    }
}
