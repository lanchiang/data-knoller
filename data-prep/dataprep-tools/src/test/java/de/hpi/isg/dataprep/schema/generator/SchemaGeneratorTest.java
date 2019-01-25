package de.hpi.isg.dataprep.schema.generator;

import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.schema.transforms.TransMergeAttribute;
import de.hpi.isg.dataprep.schema.transforms.TransSplitAttribute;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lan.jiang
 * @since 1/24/19
 */
public class SchemaGeneratorTest {

    private final static String dataPath = "./src/test/resources/pokemon.csv";

    private static Dataset<Row> dataset;

    @BeforeClass
    public static void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        dataset = SparkSession.builder().appName("Renew schema test").master("local")
                .getOrCreate().read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(dataPath);
        dataset.printSchema();
    }

    @Test
    public void testAddTwoTransforms() {
        List<Transform> transforms = new ArrayList<>();

        Attribute sourceAtrribute = new Attribute(new StructField("date", DataTypes.StringType, true, Metadata.empty()));
        Attribute targetAttribute1 = new Attribute(new StructField("date1", DataTypes.StringType, true, Metadata.empty()));
        Attribute targetAttribute2 = new Attribute(new StructField("date2", DataTypes.StringType, true, Metadata.empty()));
        Attribute targetAttribute3 = new Attribute(new StructField("date3", DataTypes.StringType, true, Metadata.empty()));
        Attribute[] targetAttributes = new Attribute[3];
        targetAttributes[0] = targetAttribute1;
        targetAttributes[1] = targetAttribute2;
        targetAttributes[2] = targetAttribute3;

        Transform splitAttr = new TransSplitAttribute(sourceAtrribute, targetAttributes);
        transforms.add(splitAttr);

        Attribute sourceAttribute1 = new Attribute(new StructField("id", DataTypes.StringType, true, Metadata.empty()));
        Attribute sourceAttribute2 = new Attribute(new StructField("identifier", DataTypes.StringType, true, Metadata.empty()));
        Attribute sourceAttribute3 = new Attribute(new StructField("base_experience", DataTypes.StringType, true, Metadata.empty()));
        Attribute targetAttribute = new Attribute(new StructField("merge", DataTypes.StringType, true, Metadata.empty()));
        Attribute[] sourceAttributes = new Attribute[3];
        sourceAttributes[0] = sourceAttribute1;
        sourceAttributes[1] = sourceAttribute2;
        sourceAttributes[2] = sourceAttribute3;

        Transform mergeAttr = new TransMergeAttribute(sourceAttributes, targetAttribute);
        transforms.add(mergeAttr);

        Schema sourceSchema = new Schema(dataset.schema());
        SchemaGenerator schemaGenerator = new SchemaGenerator(sourceSchema, transforms);
        schemaGenerator.constructTargetSchema();

        SchemaMapping schemaMapping = schemaGenerator.createSchemaMapping();
    }
}
