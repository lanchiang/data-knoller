package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.metadata.LanguageMetadata;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.DetectLanguagePreparator;
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator;
import org.apache.spark.sql.Encoders;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class DetectLanguageTest extends PreparatorTest {

    @Test
    public void testLanguageDetection() throws Exception {
        AbstractPreparator abstractPreparator = new DetectLanguagePreparator("stemlemma", 5);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);
        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);
        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());

        Map<Integer, LanguageMetadata.LanguageEnum> langMapping = new HashMap<>();
        langMapping.put(0, LanguageMetadata.LanguageEnum.SPANISH);
        langMapping.put(1, LanguageMetadata.LanguageEnum.ENGLISH);
        Assert.assertTrue(pipeline.getMetadataRepository().containByValue(new LanguageMetadata("stemlemma", langMapping)));
    }

    @Test
    public void testUnsupportedLanguages() throws Exception {
        AbstractPreparator abstractPreparator = new DetectLanguagePreparator("stemlemma", 2);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getErrorRepository().getPrintedReady().forEach(System.out::println);

        List<ErrorLog> realErrorLogs = pipeline.getErrorRepository().getErrorLogs();
        Assert.assertEquals(2, realErrorLogs.size());

        PreparationErrorLog italianError = (PreparationErrorLog) realErrorLogs.get(0);
        Assert.assertEquals("DetectLanguagePreparator", italianError.getPreparation().getName());
        Assert.assertEquals("LanguageMetadata class org.languagetool.language.Italian not supported",
                            italianError.getErrorMessage());
        Assert.assertEquals("stemlemma", italianError.getValue());

        PreparationErrorLog bretonError = (PreparationErrorLog) realErrorLogs.get(1);
        Assert.assertEquals("DetectLanguagePreparator", bretonError.getPreparation().getName());
        Assert.assertEquals("LanguageMetadata class org.languagetool.language.Breton not supported",
                bretonError.getErrorMessage());
        Assert.assertEquals("stemlemma", bretonError.getValue());
    }



}
