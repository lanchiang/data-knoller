package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.DecisionEngine;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.metadata.PreambleExistence;
import de.hpi.isg.dataprep.metadata.PropertyDataType;
import de.hpi.isg.dataprep.metadata.PropertyExistence;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.MergeAttribute;
import de.hpi.isg.dataprep.preparators.define.MergeUtil;
import de.hpi.isg.dataprep.preparators.implementation.DefaultMergeAttributeImpl;
import de.hpi.isg.dataprep.schema.transforms.TransMergeAttribute;
import de.hpi.isg.dataprep.util.DataType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.*;

import java.nio.channels.Pipe;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MergeAttributeTest {
	protected static Dataset<Row> dataset;
	protected static AbstractPipeline pipeline;
	protected static DataContext dataContext;
    protected static  DataContext benkeContext;
	protected static  DataContext benkeContextFull;

	protected static  DataContext restaurantsContext;

	static FileLoadDialect restaurants;
	static FileLoadDialect pokemons;

	protected static org.apache.spark.sql.types.Metadata emptyMetadata = org.apache.spark.sql.types.Metadata.empty();


	@BeforeClass
	public static void setUp() {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		pokemons = new DialectBuilder()
			.hasHeader(true)
			.inferSchema(true)
			.url("./src/test/resources/pokemon.csv")
			.buildDialect();

		restaurants = new DialectBuilder()
			.hasHeader(true)
			.delimiter("\t")
			.inferSchema(true)
			.url("./src/test/resources/restaurants.tsv")
			.buildDialect();

        FileLoadDialect benke = new DialectBuilder()
                .hasHeader(true)
                .delimiter(";")
                .inferSchema(true)
                .url("./src/test/resources/BenkeKarsch.short.csv")
                .buildDialect();

//		FileLoadDialect benkeFull = new DialectBuilder()
//			.hasHeader(true)
//			.delimiter(";")
//			.inferSchema(true)
//			.url("./src/test/resources/BenkeKarsch.csv")
//			.buildDialect();

		SparkDataLoader dataLoader = new FlatFileDataLoader(pokemons);
		dataContext = dataLoader.load();

		SparkDataLoader dataLoader1 = new FlatFileDataLoader(restaurants);
		restaurantsContext = dataLoader1.load();

        SparkDataLoader dataLoader2 = new FlatFileDataLoader(benke,createTargetMetadataManuallyBenke(),createTransformsManuallyBenke());
        benkeContext = dataLoader2.load();

//        SparkDataLoader dataLoader3 = new FlatFileDataLoader(benkeFull,createTargetMetadataManuallyBenke(),createTransformsManuallyBenke());
//        benkeContextFull = dataLoader3.load();
	}

	@Before
	public void cleanUpPipeline() {
		pipeline = new Pipeline(dataContext);
	}

	@Test
	public void testAttributeMerge() throws Exception {

		List<String> columns = new ArrayList<>();
		columns.add("id");
		columns.add("species_id");

		List<String> columns2 = new ArrayList<>();
		columns2.add("stemlemma");
		columns2.add("stemlemma2");

		List<String> columns3 = new ArrayList<>();
		columns3.add("stemlemma");
		columns3.add("stemlemma_wrong");


		AbstractPreparator abstractPreparator = new MergeAttribute(columns,"");
		AbstractPreparation preparation = new Preparation(abstractPreparator);
		pipeline.addPreparation(preparation);
		pipeline.addPreparation(new Preparation(new MergeAttribute(columns2, "")));
		pipeline.addPreparation(new Preparation(new MergeAttribute(columns3, "")));
		pipeline.executePipeline();
	}

	@Test
	public void mergeAdressTest() throws Exception{
		pipeline = new Pipeline(dataContext);
		List<String> columns2 = new ArrayList<>();
		columns2.add("stemlemma");
		columns2.add("stemlemma2");
		AbstractPreparator abstractPreparator = new MergeAttribute(columns2, " ");
		AbstractPreparation preparation = new Preparation(abstractPreparator);
		pipeline.addPreparation(preparation);
		pipeline.executePipeline();
	}

	@Ignore
	@Test
    public void mergeUrlTest() throws Exception{
	    pipeline = new Pipeline(benkeContextFull);
        List<String> columns = new ArrayList<>();
        columns.add("url");
        columns.add("biourl");
        AbstractPreparator abstractPreparator = new MergeAttribute(columns, " ");
        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
    }

	@Test
	public void testAplicabilityFunc() throws Exception{

		Set<Metadata> targetMetadata = createTargetMetadataManually();

//        SparkDataLoader dataLoader = new FlatFileDataLoader(pokemons, targetMetadata, schemaMapping);
		List<Transform> transforms = createTransformsManually();
		SparkDataLoader dataLoader = new FlatFileDataLoader(pokemons, targetMetadata, transforms);
		pipeline = new Pipeline(dataLoader.load());
		List<String> columns = new ArrayList<>();
//		columns.add("stemlemma");
//		columns.add("stemlemma2");
		AbstractPreparator abstractPreparator = new MergeAttribute();
		AbstractPreparation preparation = new Preparation(abstractPreparator);
		pipeline.addPreparation(preparation);

		//Dataset<Row> dataset = restaurantsContext.getDataFrame();
		//List<String> columns = new ArrayList<>();
		//columns.add("address");
		//columns.add("city");
//		pipeline.executePipeline();
//		pipeline.getDataset().show();
		DecisionEngine decisionEngine = DecisionEngine.getInstance();
		AbstractPreparator actualPreparator = decisionEngine.selectBestPreparator(pipeline);
		pipeline = new Pipeline(dataLoader.load());
		pipeline.addPreparation(new Preparation(actualPreparator));
		pipeline.executePipeline();
		//AbstractPreparator abstractPreparator = new MergeAttribute(columns, " ");
		//System.out.println(abstractPreparator.calApplicability(null,dataset,null));
	}

	@Ignore
	@Test
    public void testMergeDateFunc() throws Exception{
        pipeline = new Pipeline(benkeContextFull);

		List<String> columns = new ArrayList<>();
		columns.add("dateofbirth_day");
		columns.add("dateofbirth_month");
		columns.add("dateofbirth_year");

		MergeAttribute abstractPreparator = new MergeAttribute(columns,".");

		//dateofbirth_year|dateofbirth_month|dateofbirth_day
        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
//        DecisionEngine decisionEngine = DecisionEngine.getInstance();
//        AbstractPreparator selectedPrep =  decisionEngine.selectBestPreparator(pipeline);
//        pipeline = new Pipeline(benkeContext);
//        pipeline.addPreparation(new Preparation(selectedPrep));
        pipeline.executePipeline();
    }
	@Test
	public void testMergeDateFuncAppl() throws Exception{
		pipeline = new Pipeline(benkeContext);

		MergeAttribute abstractPreparator = new MergeAttribute();

		//dateofbirth_year|dateofbirth_month|dateofbirth_day
		AbstractPreparation preparation = new Preparation(abstractPreparator);
		pipeline.addPreparation(preparation);
        DecisionEngine decisionEngine = DecisionEngine.getInstance();
        AbstractPreparator selectedPrep =  decisionEngine.selectBestPreparator(pipeline);
        pipeline = new Pipeline(benkeContext);
        pipeline.addPreparation(new Preparation(selectedPrep));
		pipeline.executePipeline();
	}

	@Test
	public void testMerge() throws Exception{
		String a = "Hallo";
		String b = "Welt";
		String connector = "-";
		String expectedResult = a + connector + b;
		String result = MergeUtil.merge(a,b,connector);

		Assert.assertEquals(expectedResult,result);
	}

//	@Test
//	public void testMergeConflict() throws Exception{
//		String a = "Hallo";
//		String b = "Welt";
//		String expectedResult = a + " " + b;
//		String result = MergeUtil.merge(a,b,MergeUtil::handleMergeConflicts);
//
//		Assert.assertEquals(expectedResult,result);
//	}


	private static Set<Metadata> createTargetMetadataManually() {
		Set<Metadata> targetMetadata = new HashSet<>();
		targetMetadata.add(new PreambleExistence(false));
		targetMetadata.add(new PropertyExistence("stem", true));
		targetMetadata.add(new PropertyDataType("stem", DataType.PropertyType.STRING));
		return targetMetadata;
	}
	private static Set<Metadata> createTargetMetadataManuallyBenke() {
		Set<Metadata> targetMetadata = new HashSet<>();
		targetMetadata.add(new PreambleExistence(false));
		targetMetadata.add(new PropertyExistence("firstname", true));
		targetMetadata.add(new PropertyDataType("firstname", DataType.PropertyType.STRING));
		return targetMetadata;
	}
	private static List<Transform> createTransformsManually() {
		// generate schema mapping
		List<Transform> transforms = new ArrayList<>();
        Attribute[] sourceAttributes = new Attribute[]{
                new Attribute(new StructField("stemlemma", DataTypes.StringType, true, emptyMetadata))
                , new Attribute(new StructField("stemlemma2", DataTypes.StringType, true, emptyMetadata))
        };
		Attribute targetAttribute = new Attribute(new StructField("stem",DataTypes.StringType,true,  emptyMetadata));
		Transform mergeAttribute = new TransMergeAttribute(sourceAttributes,targetAttribute);
		transforms.add(mergeAttribute);

		return transforms;
	}
	private static List<Transform> createTransformsManuallyBenke() {
		// generate schema mapping
		List<Transform> transforms = new ArrayList<>();
        Attribute[] sourceAttributes = new Attribute[]{
                new Attribute(new StructField("firstname", DataTypes.StringType, true, emptyMetadata))
                , new Attribute(new StructField("firstname", DataTypes.StringType, true, emptyMetadata))
        };
		Attribute targetAttribute = new Attribute(new StructField("lastname",DataTypes.StringType,true,  emptyMetadata));
		Transform mergeAttribute = new TransMergeAttribute(sourceAttributes,targetAttribute);
		transforms.add(mergeAttribute);

		return transforms;
	}


	@Test
	public void testAttributeMergeWithInt() throws Exception {

//		List<Integer> columns = new ArrayList<>();
//		columns.add(0);
//		columns.add(1);
//		AbstractPreparator abstractPreparator = new MergeAttribute(columns, "|");
//		AbstractPreparation preparation = new Preparation(abstractPreparator);
////		pipeline.executePipeline();
	//	pipeline.getDataset().show();
	}

}
