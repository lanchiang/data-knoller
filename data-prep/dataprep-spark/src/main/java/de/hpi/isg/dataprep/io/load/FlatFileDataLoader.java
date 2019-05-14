package de.hpi.isg.dataprep.io.load;

import de.hpi.isg.dataprep.Metadata;
import de.hpi.isg.dataprep.io.context.DataContext;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.objects.MetadataOld;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.schema.generator.SchemaMappingGenerator;
import org.apache.spark.sql.DataFrameReader;

import java.util.List;
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

    public FlatFileDataLoader(FileLoadDialect dialect, Set<Metadata> targetMetadata, List<Transform> transforms) {
        this.dialect = dialect;
        this.targetMetadata = targetMetadata;
        this.transforms = transforms;
    }

    @Override
    public DataContext load() {
        DataFrameReader dataFrameReader = super.createDataFrameReader();
        dataFrame = dataFrameReader.csv(dialect.getUrl());
        // TODO: split file ?
//        return new DataContext(dataFrame, dialect);

        this.schemaMapping = createSchemaMapping();

        return new DataContext(dataFrame, dialect, targetMetadata, schemaMapping);
    }

    @Override
    protected SchemaMapping createSchemaMapping() {
        if (transforms == null) {
            return null;
        }

        Schema sourceSchema = new Schema(dataFrame.schema());
        SchemaMappingGenerator generator = new SchemaMappingGenerator(sourceSchema, transforms);
        generator.constructTargetSchema();
        return generator.createSchemaMapping();
    }

    /**
     * If delimiter is not specified, try to infer it.
     */
    private void inferDelimiter() {
    }
}
