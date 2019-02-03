package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.metadata.LanguageMetadata;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator;
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
        AbstractPreparator abstractPreparator = new LemmatizePreparator("stemlemma");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.executePipeline();

        pipeline.getMetadataRepository().getMetadataPool().add(new LanguageMetadata("stemlemma", LanguageMetadata.LanguageEnum.ENGLISH));
        // Needs to be done outside of executePipeline to avoid overwriting english lang metadata...
        pipeline.addPreparation(preparation);
        preparation.getAbstractPreparator().execute();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);
        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);
        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());

        List<String> actualStemlemma = pipeline.getRawData().select("stemlemma_lemmatized").as(Encoders.STRING()).collectAsList();
        List<String> expected = Arrays.asList("worst", "best", "you be", "amazingly", "I be", "be", "going", "war", "Fred s house", "succeed");
        Assert.assertEquals(expected, actualStemlemma);
    }

    @Test
    public void testValidSpanishColumn() throws Exception {
        AbstractPreparator abstractPreparator = new LemmatizePreparator("stemlemma");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.executePipeline();

        pipeline.getMetadataRepository().getMetadataPool().add(new LanguageMetadata("stemlemma", LanguageMetadata.LanguageEnum.SPANISH));
        // Needs to be done outside of executePipeline to avoid overwriting english lang metadata...
        pipeline.addPreparation(preparation);
        preparation.getAbstractPreparator().execute();

        pipeline.getRawData().show();
    }

    @Test
    public void testMultipleValidColumns() throws Exception {
        String[] parameters = new String[]{"stemlemma", "stemlemma2"};
        AbstractPreparator abstractPreparator = new LemmatizePreparator(parameters);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.executePipeline();

        pipeline.getMetadataRepository().getMetadataPool().add(new LanguageMetadata("stemlemma", LanguageMetadata.LanguageEnum.ENGLISH));
        pipeline.getMetadataRepository().getMetadataPool().add(new LanguageMetadata("stemlemma2", LanguageMetadata.LanguageEnum.ENGLISH));
        // Needs to be done outside of executePipeline to avoid overwriting english lang metadata...
        pipeline.addPreparation(preparation);
        preparation.getAbstractPreparator().execute();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);
        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());

        List<String> actualStemlemma = pipeline.getRawData().select("stemlemma_lemmatized").as(Encoders.STRING()).collectAsList();
        List<String> actualStemlemma2 = pipeline.getRawData().select("stemlemma2_lemmatized").as(Encoders.STRING()).collectAsList();
        List<String> expected = Arrays.asList("worst", "best", "you be", "amazingly", "I be", "be", "going", "war", "Fred s house", "succeed");

        Assert.assertEquals(expected, actualStemlemma);
        Assert.assertEquals(expected, actualStemlemma2);
    }


    @Test
    public void testInvalidColumn() throws Exception {
        AbstractPreparator abstractPreparator = new LemmatizePreparator("stemlemma_wrong");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.executePipeline();

        pipeline.getMetadataRepository().getMetadataPool().add(new LanguageMetadata("stemlemma_wrong", LanguageMetadata.LanguageEnum.ENGLISH));
        // Needs to be done outside of executePipeline to avoid overwriting english lang metadata...
        pipeline.addPreparation(preparation);
        preparation.getAbstractPreparator().execute();

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
        AbstractPreparator abstractPreparator = new LemmatizePreparator("this_column_does_not_exist");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

}
