package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Encoders;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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

        List<String> actualStemlemma = pipeline.getRawData().select("stemlemma_lemmatized").as(Encoders.STRING()).collectAsList();
        List<String> expected = Arrays.asList("worst", "best", "you be", "amazingly", "I be", "be", "go", "war", "Fred 's house", "succeed");
        Assert.assertEquals(expected, actualStemlemma);
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

        List<String> actualStemlemma = pipeline.getRawData().select("stemlemma_lemmatized").as(Encoders.STRING()).collectAsList();
        List<String> actualStemlemma2 = pipeline.getRawData().select("stemlemma2_lemmatized").as(Encoders.STRING()).collectAsList();
        List<String> expected = Arrays.asList("worst", "best", "you be", "amazingly", "I be", "be", "go", "war", "Fred 's house", "succeed");

        Assert.assertEquals(expected, actualStemlemma);
        Assert.assertEquals(expected, actualStemlemma2);
    }


    @Test
    public void testInvalidColumn() throws Exception {
        Preparator preparator = new LemmatizePreparator("stemlemma_wrong");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> realErrorLogs = pipeline.getErrorRepository().getErrorLogs();
        Assert.assertEquals(1, realErrorLogs.size());

        PreparationErrorLog emptyStringError = (PreparationErrorLog) realErrorLogs.get(0);
        Assert.assertEquals("LemmatizePreparator", emptyStringError.getPreparation().getName());
        Assert.assertEquals("Empty field", emptyStringError.getErrorMessage());
        Assert.assertEquals("  ", emptyStringError.getValue());
    }

//    @Test(expected = SparkException.class)
    @Test(expected = PreparationHasErrorException.class)
    public void testMissingColumn() throws Exception {
        Preparator preparator = new LemmatizePreparator("this_column_does_not_exist");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

}
