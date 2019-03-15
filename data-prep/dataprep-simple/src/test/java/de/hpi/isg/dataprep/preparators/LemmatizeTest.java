package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.metadata.LanguageMetadata;
import de.hpi.isg.dataprep.metadata.LemmatizedMetadata;
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
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

/**
 * Created by danthe on 26.11.18.
 */
public class LemmatizeTest extends PreparatorTest {

    @Ignore
    @Test
    public void testValidColumn() throws Exception {
        AbstractPreparator abstractPreparator = new LemmatizePreparator("stemlemma");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.executePipeline();

        Map<Integer, LanguageMetadata.LanguageEnum> langMapping = new HashMap<>();
        langMapping.put(0, null);
        langMapping.put(1, LanguageMetadata.LanguageEnum.ENGLISH);
        pipeline.getMetadataRepository().getMetadataPool().add(
                new LanguageMetadata("stemlemma", langMapping, 5)
        );
        // Needs to be done outside of executePipeline to avoid overwriting english lang metadata...
        pipeline.addPreparation(preparation);
        ExecutionContext executionContext = preparation.getAbstractPreparator().execute(pipeline.getDataset());

//        pipeline.getDataset().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);
        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);
        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());

//        List<String> actualStemlemma = pipeline.getDataset().select("stemlemma_lemmatized").as(Encoders.STRING()).collectAsList();
        List<String> actualStemlemma = executionContext.newDataFrame().select("stemlemma_lemmatized").as(Encoders.STRING()).collectAsList();
        List<String> expected = Arrays.asList("está abierto", "murió en 1923", "Qué haces en mi casa?", "yo estoy muy cansado", "Vete al diablo!", "be", "amazingly", "you be", "Fred s house", "succeed");
        Assert.assertEquals(expected, actualStemlemma);
        Assert.assertTrue(pipeline.getMetadataRepository().containByValue(new LemmatizedMetadata("stemlemma")));
    }

    @Ignore
    @Test
    public void testValidSpanishColumn() throws Exception {
        AbstractPreparator abstractPreparator = new LemmatizePreparator("stemlemma");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.executePipeline();

        Map<Integer, LanguageMetadata.LanguageEnum> langMapping = new HashMap<>();
        langMapping.put(0, LanguageMetadata.LanguageEnum.SPANISH);
        langMapping.put(1, null);
        pipeline.getMetadataRepository().getMetadataPool().add(
                new LanguageMetadata("stemlemma", langMapping, 5)
        );
        // Needs to be done outside of executePipeline to avoid overwriting english lang metadata...
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
//        ExecutionContext executionContext = preparation.getAbstractPreparator().execute(pipeline.getDataset());

//        pipeline.getDataset().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);
        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);
        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());

        List<String> actualStemlemma = pipeline.getDataset().select("stemlemma_lemmatized").as(Encoders.STRING()).collectAsList();
//        List<String> actualStemlemma = executionContext.newDataFrame().select("stemlemma_lemmatized").as(Encoders.STRING()).collectAsList();
        List<String> expected = Arrays.asList("estar abrir", "morir en 1923", "qué hacer en mi casa", "yo estar muy cansar", "vetar a+el diablo", "are", "amazingly", "You are", "Fred's house", "succeeded");
        Assert.assertEquals(expected, actualStemlemma);
        Assert.assertTrue(pipeline.getMetadataRepository().containByValue(new LemmatizedMetadata("stemlemma")));
    }

    @Ignore
    @Test
    public void testInvalidColumn() throws Exception {
        AbstractPreparator abstractPreparator = new LemmatizePreparator("stemlemma_wrong");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.executePipeline();

        Map<Integer, LanguageMetadata.LanguageEnum> langMapping = new HashMap<>();
        langMapping.put(0, LanguageMetadata.LanguageEnum.ENGLISH);
        pipeline.getMetadataRepository().getMetadataPool().add(
                new LanguageMetadata("stemlemma_wrong", langMapping, 10)
        );
        // Needs to be done outside of executePipeline to avoid overwriting english lang metadata...
        pipeline.addPreparation(preparation);
        preparation.getAbstractPreparator().execute(pipeline.getDataset());

//        pipeline.getDataset().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> realErrorLogs = pipeline.getErrorRepository().getErrorLogs();
        Assert.assertEquals(1, realErrorLogs.size());

        PreparationErrorLog emptyStringError = (PreparationErrorLog) realErrorLogs.get(0);
        Assert.assertEquals("LemmatizePreparator", emptyStringError.getPreparation().getName());
        Assert.assertEquals("Empty field", emptyStringError.getErrorMessage());
        Assert.assertEquals("  ", emptyStringError.getValue());
    }

    @Ignore
    //    @Test(expected = SparkException.class)
    @Test(expected = PreparationHasErrorException.class)
    public void testMissingColumn() throws Exception {
        AbstractPreparator abstractPreparator = new LemmatizePreparator("this_column_does_not_exist");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

//        pipeline.getDataset().show();
        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

}
