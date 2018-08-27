package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.implementation.defaults.DefaultChangePropertyDataTypeImpl;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.Pipeline;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.DatePattern;
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

import java.text.ParseException;
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
    public void testChangeToIntegerErrorLog() throws Exception {
        Preparator preparator = new ChangePropertyDataType(new DefaultChangePropertyDataTypeImpl());
        ((ChangePropertyDataType) preparator).setPropertyName("id");
        ((ChangePropertyDataType) preparator).setSourceType(DataType.PropertyType.STRING);
        ((ChangePropertyDataType) preparator).setTargetType(DataType.PropertyType.INTEGER);

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorLog errorLog1 = new PreparationErrorLog(preparation, "three", new NumberFormatException("For input string: \"three\""));
        ErrorLog errorLog2 = new PreparationErrorLog(preparation, "six", new NumberFormatException("For input string: \"six\""));
        ErrorLog errorLog3 = new PreparationErrorLog(preparation, "ten", new NumberFormatException("For input string: \"ten\""));

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
        ((ChangePropertyDataType) preparator).setTargetType(DataType.PropertyType.STRING);
        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testChangeToDoubleErrorLog() throws Exception {
        Preparator preparator = new ChangePropertyDataType(new DefaultChangePropertyDataTypeImpl());
        ((ChangePropertyDataType) preparator).setPropertyName("id");
        ((ChangePropertyDataType) preparator).setTargetType(DataType.PropertyType.DOUBLE);

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorLog errorLog1 = new PreparationErrorLog(preparation, "three", new NumberFormatException("For input string: \"three\""));
        ErrorLog errorLog2 = new PreparationErrorLog(preparation, "six", new NumberFormatException("For input string: \"six\""));
        ErrorLog errorLog3 = new PreparationErrorLog(preparation, "ten", new NumberFormatException("For input string: \"ten\""));

        trueErrorlogs.add(errorLog1);
        trueErrorlogs.add(errorLog2);
        trueErrorlogs.add(errorLog3);
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testChangeToDateErrorLog() throws Exception {
        Preparator preparator = new ChangePropertyDataType(new DefaultChangePropertyDataTypeImpl());
        ((ChangePropertyDataType) preparator).setPropertyName("date");
        ((ChangePropertyDataType) preparator).setTargetType(DataType.PropertyType.DATE);
        ((ChangePropertyDataType) preparator).setSourceDatePattern(DatePattern.DatePatternEnum.YearMonthDay);
        ((ChangePropertyDataType) preparator).setTargetDatePattern(DatePattern.DatePatternEnum.MonthDayYear);
        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorLog errorLog1 = new PreparationErrorLog(preparation, "thisIsDate", new ParseException("Unparseable date: \"thisIsDate\"", 0));
        ErrorLog errorLog2 = new PreparationErrorLog(preparation, "12-11-1989", new ParseException("Unparseable date: \"12-11-1989\"", 10));
        ErrorLog errorLog3 = new PreparationErrorLog(preparation, "2014-13-31", new ParseException("Unparseable date: \"2014-13-31\"", 10));
        ErrorLog errorLog4 = new PreparationErrorLog(preparation, "2000-01-32", new ParseException("Unparseable date: \"2000-01-32\"", 10));

        trueErrorlogs.add(errorLog1);
        trueErrorlogs.add(errorLog2);
        trueErrorlogs.add(errorLog3);
        trueErrorlogs.add(errorLog4);
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }
}
