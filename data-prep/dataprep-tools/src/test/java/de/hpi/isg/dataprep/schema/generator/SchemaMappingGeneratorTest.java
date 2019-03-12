package de.hpi.isg.dataprep.schema.generator;

import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Schema;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.schema.transforms.TransAddAttribute;
import de.hpi.isg.dataprep.schema.transforms.TransDeleteAttribute;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author lan.jiang
 * @since 1/24/19
 */
public class SchemaMappingGeneratorTest {

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
//        dataset.printSchema();
    }

    @Test
    public void testAddFourTransforms() {
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
        Attribute sourceAttribute2 = new Attribute(new StructField("date3", DataTypes.StringType, true, Metadata.empty()));
        Attribute sourceAttribute3 = new Attribute(new StructField("base_experience", DataTypes.StringType, true, Metadata.empty()));
        Attribute targetAttribute = new Attribute(new StructField("merge", DataTypes.StringType, true, Metadata.empty()));
        Attribute[] sourceAttributes = new Attribute[3];
        sourceAttributes[0] = sourceAttribute1;
        sourceAttributes[1] = sourceAttribute2;
        sourceAttributes[2] = sourceAttribute3;

        Transform mergeAttr = new TransMergeAttribute(sourceAttributes, targetAttribute);
        transforms.add(mergeAttr);

        sourceAtrribute = new Attribute(new StructField("date", DataTypes.StringType, true, Metadata.empty()));
        Transform deleteAttr = new TransDeleteAttribute(sourceAtrribute);
        transforms.add(deleteAttr);

        targetAttribute = new Attribute(new StructField("added", DataTypes.IntegerType, true, Metadata.empty()));
        Transform addAttr = new TransAddAttribute(targetAttribute);
        transforms.add(addAttr);

        Schema sourceSchema = new Schema(dataset.schema());
        SchemaMappingGenerator schemaMappingGenerator = new SchemaMappingGenerator(sourceSchema, transforms);
        schemaMappingGenerator.constructTargetSchema();

        SchemaMapping schemaMapping = schemaMappingGenerator.createSchemaMapping();
//        schemaMapping.print();

        List<Attribute> targetSchemaAttributes = new LinkedList<>();
        targetSchemaAttributes.add(new Attribute(new StructField("merge", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("id", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("identifier", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("height", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("weight", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("base_experience", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("order", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("is_default", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date1", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date2", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date3", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma2", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma_wrong", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("added", DataTypes.IntegerType, true, Metadata.empty())));
        Schema expectedTargetSchema = new Schema(targetSchemaAttributes);
        Assert.assertEquals(expectedTargetSchema, schemaMapping.getTargetSchema());
    }

    @Test
    public void testTwoAdd() {
        List<Transform> transforms = new ArrayList<>();
        Attribute targetAttribute = new Attribute(new StructField("added1", DataTypes.IntegerType, true, Metadata.empty()));
        Transform addAttr1 = new TransAddAttribute(targetAttribute);
        transforms.add(addAttr1);

        targetAttribute = new Attribute(new StructField("added2", DataTypes.StringType, true, Metadata.empty()));
        Transform addAttr2 = new TransAddAttribute(targetAttribute);
        transforms.add(addAttr2);

        Schema sourceSchema = new Schema(dataset.schema());
        SchemaMappingGenerator schemaMappingGenerator = new SchemaMappingGenerator(sourceSchema, transforms);
        schemaMappingGenerator.constructTargetSchema();

        SchemaMapping schemaMapping = schemaMappingGenerator.createSchemaMapping();

        List<Attribute> targetSchemaAttributes = new LinkedList<>();
        targetSchemaAttributes.add(new Attribute(new StructField("id", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("identifier", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("height", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("weight", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("base_experience", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("order", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("is_default", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma2", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma_wrong", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("added1", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("added2", DataTypes.StringType, true, Metadata.empty())));
        Schema expectedTargetSchema = new Schema(targetSchemaAttributes);
        Assert.assertEquals(expectedTargetSchema, schemaMapping.getTargetSchema());
    }

    @Test
    public void testTwoDelete() {
        List<Transform> transforms = new ArrayList<>();
        Attribute sourceAttribute = new Attribute(new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty()));
        Transform deleteAttr1 = new TransDeleteAttribute(sourceAttribute);
        transforms.add(deleteAttr1);

        sourceAttribute = new Attribute(new StructField("stemlemma", DataTypes.StringType, true, Metadata.empty()));
        Transform deleteAttr2 = new TransDeleteAttribute(sourceAttribute);
        transforms.add(deleteAttr2);

        Schema sourceSchema = new Schema(dataset.schema());
        SchemaMappingGenerator schemaMappingGenerator = new SchemaMappingGenerator(sourceSchema, transforms);
        schemaMappingGenerator.constructTargetSchema();

        SchemaMapping schemaMapping = schemaMappingGenerator.createSchemaMapping();

        List<Attribute> targetSchemaAttributes = new LinkedList<>();
        targetSchemaAttributes.add(new Attribute(new StructField("id", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("identifier", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("height", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("weight", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("base_experience", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("order", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("is_default", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma2", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma_wrong", DataTypes.StringType, true, Metadata.empty())));
        Schema expectedTargetSchema = new Schema(targetSchemaAttributes);
        Assert.assertEquals(expectedTargetSchema, schemaMapping.getTargetSchema());
    }

    @Test
    public void testTwoSplit() {
        List<Transform> transforms = new ArrayList<>();
        Attribute sourceAtrribute = new Attribute(new StructField("date", DataTypes.StringType, true, Metadata.empty()));
        Attribute targetAttribute1 = new Attribute(new StructField("date1", DataTypes.StringType, true, Metadata.empty()));
        Attribute targetAttribute2 = new Attribute(new StructField("date2", DataTypes.StringType, true, Metadata.empty()));
        Attribute targetAttribute3 = new Attribute(new StructField("date3", DataTypes.StringType, true, Metadata.empty()));
        Attribute[] targetAttributes = new Attribute[3];
        targetAttributes[0] = targetAttribute1;
        targetAttributes[1] = targetAttribute2;
        targetAttributes[2] = targetAttribute3;

        Transform splitAttr1 = new TransSplitAttribute(sourceAtrribute, targetAttributes);
        transforms.add(splitAttr1);

        sourceAtrribute = new Attribute(new StructField("date2", DataTypes.StringType, true, Metadata.empty()));
        targetAttribute1 = new Attribute(new StructField("date21", DataTypes.StringType, true, Metadata.empty()));
        targetAttribute2 = new Attribute(new StructField("date22", DataTypes.StringType, true, Metadata.empty()));
        targetAttributes = new Attribute[2];
        targetAttributes[0] = targetAttribute1;
        targetAttributes[1] = targetAttribute2;

        Transform splitAttr2 = new TransSplitAttribute(sourceAtrribute, targetAttributes);
        transforms.add(splitAttr2);

        Schema sourceSchema = new Schema(dataset.schema());
        SchemaMappingGenerator schemaMappingGenerator = new SchemaMappingGenerator(sourceSchema, transforms);
        schemaMappingGenerator.constructTargetSchema();

        SchemaMapping schemaMapping = schemaMappingGenerator.createSchemaMapping();

        List<Attribute> targetSchemaAttributes = new LinkedList<>();
        targetSchemaAttributes.add(new Attribute(new StructField("id", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("identifier", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("height", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("weight", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("base_experience", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("order", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("is_default", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date1", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date21", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date22", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date2", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date3", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma2", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma_wrong", DataTypes.StringType, true, Metadata.empty())));
        Schema expectedTargetSchema = new Schema(targetSchemaAttributes);
        Assert.assertEquals(expectedTargetSchema, schemaMapping.getTargetSchema());
    }

    @Test
    public void testTwoMerge() {
        List<Transform> transforms = new ArrayList<>();

        Attribute sourceAttribute1 = new Attribute(new StructField("id", DataTypes.StringType, true, Metadata.empty()));
        Attribute sourceAttribute2 = new Attribute(new StructField("identifier", DataTypes.StringType, true, Metadata.empty()));
        Attribute sourceAttribute3 = new Attribute(new StructField("base_experience", DataTypes.StringType, true, Metadata.empty()));
        Attribute targetAttribute = new Attribute(new StructField("merge", DataTypes.StringType, true, Metadata.empty()));
        Attribute[] sourceAttributes = new Attribute[3];
        sourceAttributes[0] = sourceAttribute1;
        sourceAttributes[1] = sourceAttribute2;
        sourceAttributes[2] = sourceAttribute3;

        Transform mergeAttr1 = new TransMergeAttribute(sourceAttributes, targetAttribute);
        transforms.add(mergeAttr1);

        sourceAttribute1 = new Attribute(new StructField("stemlemma", DataTypes.StringType, true, Metadata.empty()));
        sourceAttribute2 = new Attribute(new StructField("stemlemma2", DataTypes.StringType, true, Metadata.empty()));
        targetAttribute = new Attribute(new StructField("merge2", DataTypes.StringType, true, Metadata.empty()));
        sourceAttributes = new Attribute[2];
        sourceAttributes[0] = sourceAttribute1;
        sourceAttributes[1] = sourceAttribute2;

        Transform mergeAttr2 = new TransMergeAttribute(sourceAttributes, targetAttribute);
        transforms.add(mergeAttr2);

        Schema sourceSchema = new Schema(dataset.schema());
        SchemaMappingGenerator schemaMappingGenerator = new SchemaMappingGenerator(sourceSchema, transforms);
        schemaMappingGenerator.constructTargetSchema();

        SchemaMapping schemaMapping = schemaMappingGenerator.createSchemaMapping();

        List<Attribute> targetSchemaAttributes = new LinkedList<>();
        targetSchemaAttributes.add(new Attribute(new StructField("merge", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("id", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("identifier", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("height", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("weight", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("base_experience", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("order", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("is_default", DataTypes.IntegerType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("date", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("merge2", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma2", DataTypes.StringType, true, Metadata.empty())));
        targetSchemaAttributes.add(new Attribute(new StructField("stemlemma_wrong", DataTypes.StringType, true, Metadata.empty())));
        Schema expectedTargetSchema = new Schema(targetSchemaAttributes);
        Assert.assertEquals(expectedTargetSchema, schemaMapping.getTargetSchema());
    }
}
