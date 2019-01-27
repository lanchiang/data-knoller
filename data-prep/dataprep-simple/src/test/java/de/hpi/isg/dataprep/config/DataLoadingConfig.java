package de.hpi.isg.dataprep.config;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.metadata.PropertyDataType;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.schema.transforms.TransAddAttribute;
import de.hpi.isg.dataprep.schema.transforms.TransDeleteAttribute;
import de.hpi.isg.dataprep.schema.transforms.TransMergeAttribute;
import de.hpi.isg.dataprep.schema.transforms.TransSplitAttribute;
import de.hpi.isg.dataprep.util.DataType;
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

    protected static AbstractPipeline pipeline;
    protected static DataContext dataContext;
    protected static FileLoadDialect dialect;
    private static List<Transform> transforms;

    protected static org.apache.spark.sql.types.Metadata emptyMetadata = org.apache.spark.sql.types.Metadata.empty();

    @BeforeClass
    public static void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        // generate schema mapping
        transforms = new ArrayList<>();

        Attribute sourceAttribute = new Attribute(new StructField("date", DataTypes.StringType, true, emptyMetadata));
        Attribute targetAttribute1 = new Attribute(new StructField("date1", DataTypes.StringType, true, emptyMetadata));
        Attribute targetAttribute2 = new Attribute(new StructField("date2", DataTypes.StringType, true, emptyMetadata));
        Attribute targetAttribute3 = new Attribute(new StructField("date3", DataTypes.StringType, true, emptyMetadata));
        Attribute[] targetAttributes = new Attribute[3];
        targetAttributes[0] = targetAttribute1;
        targetAttributes[1] = targetAttribute2;
        targetAttributes[2] = targetAttribute3;

        Transform splitAttr = new TransSplitAttribute(sourceAttribute, targetAttributes);
        transforms.add(splitAttr);

        Attribute sourceAttribute1 = new Attribute(new StructField("id", DataTypes.StringType, true, emptyMetadata));
        Attribute sourceAttribute2 = new Attribute(new StructField("date3", DataTypes.StringType, true, emptyMetadata));
        Attribute sourceAttribute3 = new Attribute(new StructField("base_experience", DataTypes.StringType, true, emptyMetadata));
        Attribute targetAttribute = new Attribute(new StructField("merge", DataTypes.StringType, true, emptyMetadata));
        Attribute[] sourceAttributes = new Attribute[3];
        sourceAttributes[0] = sourceAttribute1;
        sourceAttributes[1] = sourceAttribute2;
        sourceAttributes[2] = sourceAttribute3;

        Transform mergeAttr = new TransMergeAttribute(sourceAttributes, targetAttribute);
        transforms.add(mergeAttr);

        sourceAttribute = new Attribute(new StructField("date", DataTypes.StringType, true, emptyMetadata));
        Transform deleteAttr = new TransDeleteAttribute(sourceAttribute);
        transforms.add(deleteAttr);

        targetAttribute = new Attribute(new StructField("added", DataTypes.IntegerType, true, emptyMetadata));
        Transform addAttr = new TransAddAttribute(targetAttribute);
        transforms.add(addAttr);

        // generate target metadata
        Set<Metadata> targetMetadata = createTargetMetadata();

        dialect = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                .url("./src/test/resources/pokemon.csv")
                .buildDialect();

//        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect, targetMetadata, schemaMapping);
        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect, targetMetadata, transforms);
        dataContext = dataLoader.load();

        dataContext.getDataFrame().show();
    }

    @Before
    public void cleanUpPipeline() {
        pipeline = new Pipeline(dataContext);
    }

    private static Set<Metadata> createTargetMetadata() {
        Set<Metadata> targetMetadata = new HashSet<>();
        targetMetadata.add(new PropertyDataType("date", DataType.PropertyType.DATE));
        return targetMetadata;
    }

    private static Set<Metadata> createTargetMetadataFromFile() {
        Set<Metadata> targetMetadata = new HashSet<>();

        // load metadata into the variable targetMetadata from a local file
        return targetMetadata;
    }

    private static List<Transform> createRandomTransform() {
        List<Transform> transforms = new LinkedList<>();

        // SchemaMappingGenerator creates random transform list and stores it in variable transforms.
        return transforms;
    }
}
