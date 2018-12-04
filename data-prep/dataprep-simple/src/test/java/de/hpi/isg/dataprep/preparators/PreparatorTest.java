package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class PreparatorTest {

    protected static Dataset<Row> dataset;
    protected static AbstractPipeline pipeline;
    protected static DataContext dataContext;
    protected static FileLoadDialect dialect;

    @BeforeClass
    public static void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        dialect = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                //.delimiter("\t")
                //x: works
                //.url("./src/test/resources/pokemon.csv")//->X
                //.url("./src/test/resources/restaurants.tsv")//->x
               //.url("./src/test/resources/test.csv")//->x
                //.url("./src/test/resources/test2.csv")//->x
                //.url("./src/test/resources/test21.csv")//->x
                // .url("./src/test/resources/test3.csv")//->x
                //.url("./src/test/resources/test4.csv")//->x
                //.url("./src/test/resources/test5.csv")//->x
                .url("./src/test/resources/test6.csv")//->x
                //.url("./src/test/resources/test7.csv")//->x
                //.url("./src/test/resources/test8.csv")//-> x,
                //.url("./src/test/resources/test9.csv")//->x
                //.url("./src/test/resources/test10.csv")//->x
                //.url("./src/test/resources/test11.csv")//->x

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
}