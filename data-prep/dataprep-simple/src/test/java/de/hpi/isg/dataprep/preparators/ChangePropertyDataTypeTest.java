package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.preparators.define.ChangeDataType;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.util.DataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/9
 */
public class ChangePropertyDataTypeTest extends PreparatorTest {

    @Test
    public void testChangeToStringErrorLog() throws Exception {
        AbstractPreparator abstractPreparator = new ChangeDataType("identifier", DataType.PropertyType.STRING);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);
//        pipeline.getDataset().show();

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testChangeToIntegerErrorLog() throws Exception {
        AbstractPreparator abstractPreparator = new ChangeDataType("id", DataType.PropertyType.STRING, DataType.PropertyType.INTEGER);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
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
    public void testChangeToDoubleErrorLog() throws Exception {
        AbstractPreparator abstractPreparator = new ChangeDataType("id", DataType.PropertyType.DOUBLE);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
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

//    @Test
//    public void testChangeToDateErrorLog() throws Exception {
//        AbstractPreparator preparator = new ChangeDataType("date", DataType.PropertyType.DATE,
//                DatePattern.DatePatternEnum.YearMonthDay, DatePattern.DatePatternEnum.MonthDayYear);
//
//        AbstractPreparation preparation = new Preparation(preparator);
//        pipeline.addPreparation(preparation);
//        pipeline.executePipeline();
//
//        List<ErrorLog> trueErrorlogs = new ArrayList<>();
//        ErrorLog errorLog1 = new PreparationErrorLog(preparation, "thisIsDate", new ParseException("Unparseable date: \"thisIsDate\"", 0));
//        ErrorLog errorLog2 = new PreparationErrorLog(preparation, "12-11-1989", new ParseException("Unparseable date: \"12-11-1989\"", 10));
//        ErrorLog errorLog3 = new PreparationErrorLog(preparation, "2014-13-31", new ParseException("Unparseable date: \"2014-13-31\"", 10));
//        ErrorLog errorLog4 = new PreparationErrorLog(preparation, "2000-01-32", new ParseException("Unparseable date: \"2000-01-32\"", 10));
//
//        trueErrorlogs.add(errorLog1);
//        trueErrorlogs.add(errorLog2);
//        trueErrorlogs.add(errorLog3);
//        trueErrorlogs.add(errorLog4);
//        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);
//
//        pipeline.getDataset().show();
//
//        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
//    }

    @Test
    public void testChangeFromIntegerToDouble() throws Exception {
        AbstractPreparator abstractPreparator = new ChangeDataType("species_id", DataType.PropertyType.INTEGER, DataType.PropertyType.DOUBLE);

        Preparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
    }

    @Test(expected = RuntimeException.class)
    public void testChangeToStringSourceTypeSpecifiedWrong() throws Exception {
        AbstractPreparator abstractPreparator = new ChangeDataType("id", DataType.PropertyType.INTEGER, DataType.PropertyType.STRING);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
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
}
