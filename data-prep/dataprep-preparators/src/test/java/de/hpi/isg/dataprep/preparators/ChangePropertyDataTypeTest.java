package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.implementation.defaults.DefaultChangePropertyDataTypeImpl;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.error.ErrorLog;
import de.hpi.isg.dataprep.model.target.Pipeline;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.model.target.error.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
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
        Preparator preparator = new ChangePropertyDataType(new DefaultChangePropertyDataTypeImpl());
        ((ChangePropertyDataType) preparator).setPropertyName("id");
        ((ChangePropertyDataType) preparator).setTargetType(ChangePropertyDataType.PropertyType.INTEGER);

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorLog errorLog1 = new PreparationErrorLog(preparation, "three", new NumberFormatException());
        ErrorLog errorLog2 = new PreparationErrorLog(preparation, "six", new NumberFormatException());
        ErrorLog errorLog3 = new PreparationErrorLog(preparation, "ten", new NumberFormatException());

        trueErrorlogs.add(errorLog1);
        trueErrorlogs.add(errorLog2);
        trueErrorlogs.add(errorLog3);
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testChangeToStringErrorLog() throws Exception {
        Preparator preparator = new ChangePropertyDataType(new DefaultChangePropertyDataTypeImpl());
        ((ChangePropertyDataType) preparator).setPropertyName("identifier");
        ((ChangePropertyDataType) preparator).setTargetType(ChangePropertyDataType.PropertyType.STRING);
        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testChangeToDateErrorLog() throws Exception {
        Preparator preparator = new ChangePropertyDataType(new DefaultChangePropertyDataTypeImpl());
        ((ChangePropertyDataType) preparator).setPropertyName("date");
        ((ChangePropertyDataType) preparator).setTargetType(ChangePropertyDataType.PropertyType.DATE);
        ((ChangePropertyDataType) preparator).setDatePattern("DD-MM-YYYY");
        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        /**
         * Here fill the true error logs.
         */
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }
}
