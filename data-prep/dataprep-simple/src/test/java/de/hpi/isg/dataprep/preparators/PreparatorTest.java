package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Pipeline;
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

    @BeforeClass
    public static void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        dataset = SparkSession.builder()
                .appName("Change property data type unit tests.")
                .master("local")
                .getOrCreate()
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("./src/test/resources/pokemon.csv");
        pipeline = new Pipeline(dataset);
    }

    @Before
    public void cleanUpPipeline() throws Exception {
        pipeline = new Pipeline(dataset);
    }
}