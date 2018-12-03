package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator;
import de.hpi.isg.dataprep.preparators.define.StemPreparator;
import org.apache.spark.sql.Encoders;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by danthe on 26.11.18.
 */
public class StemTest extends PreparatorTest {

    @Test
    public void testValidColumn() throws Exception {
        Preparator preparator = new StemPreparator("stemlemma");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);
        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);
        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());

        List<String> actualStemlemma = pipeline.getRawData().select("stemlemma_stemmed").as(Encoders.STRING()).collectAsList();
        List<String> expected = Arrays.asList("worst", "best", "You ar", "amazingli", "I am", "ar", "go", "war", "Fred 's hous", "succeed");
        Assert.assertEquals(expected, actualStemlemma);
    }

    @Test
    public void testMultipleValidColumns() throws Exception {

        String[] parameters = new String[]{"stemlemma", "stemlemma2"};
        Preparator preparator = new StemPreparator(parameters);

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);
        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());

        List<String> actualStemlemma = pipeline.getRawData().select("stemlemma_stemmed").as(Encoders.STRING()).collectAsList();
        List<String> actualStemlemma2 = pipeline.getRawData().select("stemlemma2_stemmed").as(Encoders.STRING()).collectAsList();
        List<String> expected = Arrays.asList("worst", "best", "You ar", "amazingli", "I am", "ar", "go", "war", "Fred 's hous", "succeed");

        Assert.assertEquals(expected, actualStemlemma);
        Assert.assertEquals(expected, actualStemlemma2);
    }


    @Test
    public void testInvalidColumn() throws Exception {
        Preparator preparator = new StemPreparator("stemlemma_wrong");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> realErrorLogs = pipeline.getErrorRepository().getErrorLogs();
        Assert.assertEquals(1, realErrorLogs.size());

        PreparationErrorLog emptyStringError = (PreparationErrorLog) realErrorLogs.get(0);
        Assert.assertEquals("StemPreparator", emptyStringError.getPreparation().getName());
        Assert.assertEquals("Empty field", emptyStringError.getErrorMessage());
        Assert.assertEquals("  ", emptyStringError.getValue());
    }

}
