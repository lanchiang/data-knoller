package de.hpi.isg.dataprep.model.preparators;

import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.ErrorLog;
import de.hpi.isg.dataprep.model.target.Pipeline;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/9
 */
public class ChangePropertyDataTypeTest {

    private static Dataset<Row> dataset;
    private static Pipeline pipeline;

    @BeforeClass
    public static void setUp() {
        dataset = SparkSession.builder()
                .appName("Pipeline test")
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
    public void testChangeToIntegerErrorLog() throws Exception {
        Preparator preparator = new ChangePropertyDataType("id", ChangePropertyDataType.PropertyType.INTEGER);
        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorLog errorLog1 = new ErrorLog("three", new NumberFormatException());
        errorLog1.setPipelineComponent(preparation);
        ErrorLog errorLog2 = new ErrorLog("six", new NumberFormatException());
        errorLog2.setPipelineComponent(preparation);
        ErrorLog errorLog3 = new ErrorLog("ten", new NumberFormatException());
        errorLog3.setPipelineComponent(preparation);

        trueErrorlogs.add(errorLog1);
        trueErrorlogs.add(errorLog2);
        trueErrorlogs.add(errorLog3);
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testChangeToStringErrorLog() throws Exception {
        Preparator preparator = new ChangePropertyDataType("identifier", ChangePropertyDataType.PropertyType.STRING);
        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }
}
