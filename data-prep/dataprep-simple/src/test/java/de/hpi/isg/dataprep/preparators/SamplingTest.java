package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.Sampling;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class SamplingTest {

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
                .url("./src/test/resources/uniformDist.csv")
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
    public void testSampling() throws Exception {

       // Preparator preparator = new Sampling(.33,false);
        Preparator preparator = new Sampling(5,false);
       // ((Sampling) preparator).setPercentage(0.33);

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
//        VectorAssembler assembler = new VectorAssembler()
//                .setInputCols("number")
//                .setOutputCol("number");
//        assembler.transform(dataContext.getDataFrame()).
//    new VectorAssembler().transform(dataContext.getDataFrame()).select("number").rdd()
//        VectorAssembler.assemble(dataContext.getDataFrame().select("number"));
        pipeline.getRawData().show();
//        Statistics.chiSqTest(new VectorAssembler().transform(dataContext.getDataFrame()).select("number").as("Vector")
//                ,new VectorAssembler().transform(pipeline.getRawData()).select("number").as("Vector").rdd());
        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);
        //Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }
}