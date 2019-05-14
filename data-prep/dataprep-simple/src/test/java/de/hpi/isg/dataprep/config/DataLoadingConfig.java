package de.hpi.isg.dataprep.config;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.Metadata;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.io.context.DataContext;
import de.hpi.isg.dataprep.io.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.io.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.schema.transforms.TransDeleteAttribute;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.*;

/**
 * @author lan.jiang
 * @since 1/27/19
 */
public class DataLoadingConfig {

    protected AbstractPipeline pipeline;
    protected static DataContext dataContext;
    protected static FileLoadDialect dialect;

    protected static List<Transform> transforms;
    protected static Set<Metadata> targetMetadata;

    protected static String resourcePath = "./src/test/resources/pokemon.csv";

    protected static org.apache.spark.sql.types.Metadata emptyMetadata = org.apache.spark.sql.types.Metadata.empty();

    @BeforeClass
    public static void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // generate target metadata
        targetMetadata = createTargetMetadataManually();

        dialect = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                .url(resourcePath)
                .buildDialect();

        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect, targetMetadata, transforms);
        dataContext = dataLoader.load();
    }

    public static void basicSetup() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // generate target metadata
        targetMetadata = createTargetMetadataManually();

        dialect = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                .url(resourcePath)
                .buildDialect();

        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect, targetMetadata, new ArrayList<>());
        dataContext = dataLoader.load();
    }

    @Before
    public void cleanUpPipeline() {
        pipeline = new Pipeline(dataContext);
    }

    private static Set<Metadata> createTargetMetadataManually() {
        Set<Metadata> targetMetadata = new HashSet<>();

//        targetMetadata.add(new PreambleExistence(false));
//        targetMetadata.add(new PropertyExistence("month", true));
//        targetMetadata.add(new PropertyExistence("day", true));
//        targetMetadata.add(new PropertyExistence("year", true));
//        targetMetadata.add(new PropertyDataType("month", DataType.PropertyType.STRING));
//        targetMetadata.add(new PropertyDataType("day", DataType.PropertyType.STRING));
//        targetMetadata.add(new PropertyDataType("year", DataType.PropertyType.STRING));

        return targetMetadata;
    }

    private static Set<Metadata> createTargetMetadataFromFile() {
        Set<Metadata> targetMetadata = new HashSet<>();

        // load metadata into the variable targetMetadata from a local file
        return targetMetadata;
    }

    private static List<Transform> createRandomTransforms() {
        List<Transform> transforms = new LinkedList<>();

        // SchemaMappingGenerator creates random transform list and stores it in variable transforms.
        return transforms;
    }

    protected static List<Transform> createTransformsManually() {
        // generate schema mapping
        List<Transform> transforms = new ArrayList<>();
        Attribute sourceAttribute = new Attribute(new StructField("date", DataTypes.StringType, true, emptyMetadata));
        Transform deleteAttr = new TransDeleteAttribute(sourceAttribute);
        transforms.add(deleteAttr);

        return transforms;
    }
}
