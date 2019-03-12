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
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class PreparatorTest {

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
                .url("./src/test/resources/pokemon.csv")
                .buildDialect();

//        FileLoadDialect dialect = new DialectBuilder()
//                .hasHeader(true)
//                .inferSchema(true)
//                .url("./src/test/resources/restaurants.tsv")
//                .delimiter("\t")
//                .buildDialect();

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