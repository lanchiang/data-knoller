package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.components.DecisionEngine;
import de.hpi.isg.dataprep.config.DataLoadingConfig;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.DeleteProperty;
import de.hpi.isg.dataprep.preparators.define.DetectLanguagePreparator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by danthe on 04.02.19.
 */
public class LanguageApplicabilityTest extends DataLoadingConfig {

    @Test
    public void testDetectLanguageSelected() {

        // imperfect test. Now the test is done step-by-step in debugging mode.
        DecisionEngine decisionEngine = DecisionEngine.getInstance();

        DecisionEngine.preparatorCandidates = new String[]{
                "DetectLanguagePreparator", "LemmatizePreparator"
        };
        AbstractPreparator actualPreparator = decisionEngine.selectBestPreparator(pipeline);
        AbstractPreparator expectedPreparator = new DetectLanguagePreparator("stemlemma");

        Assert.assertEquals(expectedPreparator, actualPreparator);
    }


}
