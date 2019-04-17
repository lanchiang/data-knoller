package de.hpi.isg.dataprep.config;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.framework.DecisionEngineTest;
import de.hpi.isg.dataprep.io.context.DataContext;
import de.hpi.isg.dataprep.io.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.io.load.SparkDataLoader;
import de.hpi.isg.dataprep.metadata.PreambleExistence;
import de.hpi.isg.dataprep.metadata.PropertyDataType;
import de.hpi.isg.dataprep.metadata.PropertyExistence;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.schema.transforms.TransDeleteAttribute;
import de.hpi.isg.dataprep.util.DataType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.*;

/**
 * @author Lan Jiang
 * @since 2019-04-12
 */
public class DatasetsLoadingConfig {

    protected AbstractPipeline pipeline;
    protected static DataContext dataContext;
    protected static FileLoadDialect dialect;

    protected static List<Transform> transforms;
    protected static Set<Metadata> targetMetadata;

    protected static String resourcePath = "./src/test/resources/pokemon.csv";

    protected static org.apache.spark.sql.types.Metadata emptyMetadata = org.apache.spark.sql.types.Metadata.empty();

    @Before
    public void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

//        // generate target metadata
        targetMetadata = createTargetMetadataManually();

        transforms = createEmptyTransformManually();

        dialect = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                .url(resourcePath)
                .buildDialect();

        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect, targetMetadata, transforms);
        dataContext = dataLoader.load();

        pipeline = new Pipeline(dataContext);
    }

    private static Set<Metadata> createTargetMetadataManually() {
        Set<Metadata> targetMetadata = new HashSet<>();

        targetMetadata.add(new PreambleExistence(false));
        targetMetadata.add(new PropertyExistence("month", true));
        targetMetadata.add(new PropertyExistence("day", true));
        targetMetadata.add(new PropertyExistence("year", true));
        targetMetadata.add(new PropertyDataType("month", DataType.PropertyType.STRING));
        targetMetadata.add(new PropertyDataType("day", DataType.PropertyType.STRING));
        targetMetadata.add(new PropertyDataType("year", DataType.PropertyType.STRING));

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

    protected static List<Transform> createEmptyTransformManually() {
        // generate schema mapping
        List<Transform> transforms = new ArrayList<>();
        return transforms;
    }
}
