package de.hpi.isg.dataprep.load;

import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import org.apache.spark.sql.DataFrameReader;

import java.util.Set;

/**
 * @author Lan Jiang
 * @since 2018/9/6
 */
public class FlatFileDataLoader extends SparkDataLoader {

    public FlatFileDataLoader(FileLoadDialect dialect) {
        this.dialect = dialect;
        this.getDialect().setTableName(dialect.getTableName() == null ? "Default dataset name" : dialect.getTableName());
    }

    public FlatFileDataLoader(FileLoadDialect dialect, Set<Metadata> targetMetadata, SchemaMapping schemaMapping) {
        this.dialect = dialect;
        this.targetMetadata = targetMetadata;
        this.schemaMapping = schemaMapping;
    }

    @Override
    public DataContext load() {
        DataFrameReader dataFrameReader = super.createDataFrameReader();
        dataFrame = dataFrameReader.csv(dialect.getUrl());
        // TODO: split file ?
//        return new DataContext(dataFrame, dialect);
        return new DataContext(dataFrame, dialect, targetMetadata, schemaMapping);
    }

    /**
     * If delimiter is not specified, try to infer it.
     */
    private void inferDelimiter() {
    }
}
