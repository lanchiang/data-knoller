package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.MergeAttribute;
import de.hpi.isg.dataprep.preparators.define.Sampling;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MergeAttributeTest {
	protected static Dataset<Row> dataset;
	protected static AbstractPipeline pipeline;
	protected static DataContext dataContext;

	protected static  DataContext restaurantsContext;

	@BeforeClass
	public static void setUp() {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		FileLoadDialect dialect = new DialectBuilder()
			.hasHeader(true)
			.inferSchema(true)
			.url("./src/test/resources/pokemon.csv")
			.buildDialect();

		FileLoadDialect restaurants = new DialectBuilder()
			.hasHeader(true)
			.delimiter("\t")
			.inferSchema(true)
			.url("./src/test/resources/restaurants.tsv")
			.buildDialect();

//        FileLoadDialect dialect = new DialectBuilder()
//                .hasHeader(true)
//                .delimiter("\t")
//                .inferSchema(true)
//                .url("./src/test/resources/restaurants.tsv")
//                .buildDialect();

		SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
		dataContext = dataLoader.load();

		SparkDataLoader dataLoader1 = new FlatFileDataLoader(restaurants);
		restaurantsContext = dataLoader1.load();

//        dataContext.getDataFrame().show();
		return;
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
		pipeline.getRawData().show();
	}

	@Test
	public void mergeAdressTest() throws Exception{
		pipeline = new Pipeline(restaurantsContext);
		List<String> columns = new ArrayList<>();
		columns.add("address");
		columns.add("city");
		AbstractPreparator abstractPreparator = new MergeAttribute(columns, " ");
		AbstractPreparation preparation = new Preparation(abstractPreparator);
		pipeline.addPreparation(preparation);
		pipeline.executePipeline();
		pipeline.getRawData().show();
	}

	@Test
	public void testAplicabilityFunc() throws Exception{
		//Dataset<Row> dataset = restaurantsContext.getDataFrame();
		//List<String> columns = new ArrayList<>();
		//columns.add("address");
		//columns.add("city");
		//AbstractPreparator abstractPreparator = new MergeAttribute(columns, " ");
		//System.out.println(abstractPreparator.calApplicability(null,dataset,null));
	}

	@Test
	public void testAttributeMergeWithInt() throws Exception {

//		List<Integer> columns = new ArrayList<>();
//		columns.add(0);
//		columns.add(1);
//		AbstractPreparator abstractPreparator = new MergeAttribute(columns, "|");
//		AbstractPreparation preparation = new Preparation(abstractPreparator);
////		pipeline.executePipeline();
	//	pipeline.getRawData().show();
	}

}
