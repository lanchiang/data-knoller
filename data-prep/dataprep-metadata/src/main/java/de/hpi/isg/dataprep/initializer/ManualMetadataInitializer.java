package de.hpi.isg.dataprep.initializer;

import de.hpi.isg.dataprep.metadata.*;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.metadata.MetadataInitializer;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.objects.TableMetadata;
import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * This implementation initializes a metadata repository with a set of hard-coded metadata.
 *
 * @author Lan Jiang
 * @since 2019-03-14
 */
public class ManualMetadataInitializer extends MetadataInitializer {

    private FileLoadDialect dialect;
    private Dataset<Row> dataset;

    public ManualMetadataInitializer(FileLoadDialect dialect, Dataset<Row> dataset) {
        this.dialect = dialect;
        this.dataset = dataset;
        this.metadataRepository = new MetadataRepository();
    }

    @Override
    public void initializeMetadataRepository() {
        CSVSourcePath csvPath = new CSVSourcePath(dialect.getUrl());
        UsedEncoding usedEncoding = new UsedEncoding(dialect.getEncoding());

        Delimiter delimiter = new Delimiter(dialect.getDelimiter(), new TableMetadata(dialect.getTableName()));
        QuoteCharacter quoteCharacter = new QuoteCharacter(dialect.getQuoteChar(), new TableMetadata(dialect.getTableName()));
        EscapeCharacter escapeCharacter = new EscapeCharacter(dialect.getEscapeChar(), new TableMetadata(dialect.getTableName()));
        HeaderExistence headerExistence = new HeaderExistence(dialect.getHasHeader().equals("true"), new TableMetadata(dialect.getTableName()));

        initMetadata = new ArrayList<>();
        initMetadata.add(delimiter);
        initMetadata.add(quoteCharacter);
        initMetadata.add(escapeCharacter);
        initMetadata.add(headerExistence);
        initMetadata.add(csvPath);
        initMetadata.add(usedEncoding);

        StructType structType = dataset.schema();

        List<Attribute> attributes = new LinkedList<>();
        Arrays.stream(structType.fields()).forEach(field -> {
            DataType dataType = field.dataType();
            String fieldName = field.name();
            PropertyDataType propertyDataType = new PropertyDataType(fieldName, de.hpi.isg.dataprep.util.DataType.getTypeFromSparkType(dataType));
            Attribute attribute = new Attribute(field);
            attributes.add(attribute);
            initMetadata.add(propertyDataType);
        });
        Schemata schemata = new Schemata("table", attributes);
        initMetadata.add(schemata);

        metadataRepository.updateMetadata(initMetadata);
    }
}
