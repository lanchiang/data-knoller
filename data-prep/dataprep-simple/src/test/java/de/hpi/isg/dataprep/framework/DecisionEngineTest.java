package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.components.DecisionEngine;
import de.hpi.isg.dataprep.config.DataLoadingConfig;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author lan.jiang
 * @since 1/20/19
 */
public class DecisionEngineTest extends DataLoadingConfig {

    @Test
    public void selectBestPreparatorTest() {
        // imperfect test. Now the test is done step-by-step in debugging mode.
        DecisionEngine decisionEngine = DecisionEngine.getInstance();
        decisionEngine.selectBestPreparator(pipeline);
    }
}
