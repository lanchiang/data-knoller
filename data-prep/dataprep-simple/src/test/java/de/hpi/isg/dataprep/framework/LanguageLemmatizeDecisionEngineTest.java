package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.components.DecisionEngine;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.config.DataLoadingConfig;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.DetectLanguagePreparator;
import de.hpi.isg.dataprep.preparators.define.LemmatizePreparator;
import de.hpi.isg.dataprep.utils.UpdateUtils;
import org.apache.spark.sql.Encoders;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by danthe on 04.02.19.
 */
public class LanguageLemmatizeDecisionEngineTest extends DataLoadingConfig {

    @Test
    public void testDetectLanguageSelected() {
        DataLoadingConfig.setUp();

        DecisionEngine decisionEngine = DecisionEngine.getInstance();

        decisionEngine.setPreparatorCandidates(new String[]{
                "DetectLanguagePreparator"
        });
        AbstractPreparator actualPreparator = decisionEngine.selectBestPreparator(pipeline);
        AbstractPreparator expectedPreparator = new DetectLanguagePreparator("stemlemma");

        Assert.assertEquals(expectedPreparator, actualPreparator);
    }

    @Test
    public void testLemmatizerSelected() throws Exception {
        DataLoadingConfig.setUp();

        AbstractPreparator abstractPreparator = new DetectLanguagePreparator("stemlemma", 5);
        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
        UpdateUtils.updateMetadata((Pipeline) pipeline, abstractPreparator);

        DecisionEngine decisionEngine = DecisionEngine.getInstance();
        decisionEngine.setPreparatorCandidates(new String[]{
                "LemmatizePreparator"
        });
        AbstractPreparator actualPreparator = decisionEngine.selectBestPreparator(pipeline);
        AbstractPreparator expectedPreparator = new LemmatizePreparator("stemlemma");

        Assert.assertEquals(expectedPreparator, actualPreparator);
    }

    @Ignore
    @Test
    public void testBoth() throws Exception {
        DataLoadingConfig.setUp();

//        DecisionEngine.preparatorCandidates = new String[]{
//                "LemmatizePreparator", "DetectLanguagePreparator"
//        };

        pipeline.addRecommendedPreparation(); // detect language
        pipeline.addRecommendedPreparation(); // lemmatize stemlemma

        List<String> actualStemlemma = pipeline.getDataset().select("stemlemma_lemmatized").as(Encoders.STRING()).collectAsList();
        List<String> expected = Arrays.asList("estar abrir", "morir en 1923", "qué hacer en mi casa", "yo estar muy cansar", "vetar a+el diablo",
                "be", "amazingly", "you be", "Fred s house", "succeed");
        Assert.assertEquals(expected, actualStemlemma);
    }


}
