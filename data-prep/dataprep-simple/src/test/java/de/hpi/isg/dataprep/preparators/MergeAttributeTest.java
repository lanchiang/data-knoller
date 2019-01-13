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

	@BeforeClass
	public static void setUp() {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		FileLoadDialect dialect = new DialectBuilder()
			.hasHeader(true)
			.inferSchema(true)
			.url("./src/test/resources/Pokemon.csv")
			.buildDialect();

//        FileLoadDialect dialect = new DialectBuilder()
//                .hasHeader(true)
//                .delimiter("\t")
//                .inferSchema(true)
//                .url("./src/test/resources/restaurants.tsv")
//                .buildDialect();

		SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
		dataContext = dataLoader.load();

//        dataContext.getDataFrame().show();
		return;
	}

	@Before
	public void cleanUpPipeline() {
		pipeline = new Pipeline(dataContext);
	}

	@Test
	public void testAttributeMerge() throws Exception {

		List<String> columns = new ArrayList<String>();
		columns.add("id");
		columns.add("identifier");
		AbstractPreparator abstractPreparator = new MergeAttribute(columns, "|");
		AbstractPreparation preparation = new Preparation(abstractPreparator);
		pipeline.addPreparation(preparation);
		pipeline.executePipeline();
		pipeline.getRawData().show();
	}

}
