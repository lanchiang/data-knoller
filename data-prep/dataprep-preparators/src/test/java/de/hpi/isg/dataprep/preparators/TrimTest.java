package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.implementation.defaults.DefaultTrimImpl;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.Pipeline;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.DataType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class TrimTest {

    private static Dataset<Row> dataset;
    private static Pipeline pipeline;

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

    @Test
    public void testTrim() throws Exception {
        Preparator preparator = new Trim(new DefaultTrimImpl());
        ((Trim) preparator).setPropertyName("identifier");

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();

        ErrorLog pipelineError = new PipelineErrorLog(pipeline,
                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
                        "propertyName='" + "identifier" + '\'' +
                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
                        '}')));

        trueErrorlogs.add(pipelineError);
        ErrorRepository trueRepository = new ErrorRepository(trueErrorlogs);

        pipeline.getRawData().show();

        Assert.assertEquals(trueRepository, pipeline.getErrorRepository());
    }
}
