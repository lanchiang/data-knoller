package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by danthe on 26.11.18.
 */
public class LemmatizeTest extends PreparatorTest {

    @Test
    public void testValidColumn() throws Exception {
        Preparator preparator = new LemmatizePreparator("stemlemma");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testMultipleValidColumns() throws Exception {

        String[] parameters = new String[]{"stemlemma", "stemlemma2"};
        Preparator preparator = new LemmatizePreparator(parameters);

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }


    @Test
    public void testInValidColumn() throws Exception {
        Preparator preparator = new LemmatizePreparator("stemlemma_wrong");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
//        String[] rows = new String[]{""};
//        for(Iterator<Row> iter = pipeline.getRawData().toLocalIterator(), int i=0; iter.hasNext();i++){
//            Row row = iter.next();
//        }
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

    @Test(expected = SparkException.class)
    public void testMissingColumn() throws Exception {
        Preparator preparator = new LemmatizePreparator("this_column_does_not_exist");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
//        String[] rows = new String[]{""};
//        for(Iterator<Row> iter = pipeline.getRawData().toLocalIterator(), int i=0; iter.hasNext();i++){
//            Row row = iter.next();
//        }
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

}
