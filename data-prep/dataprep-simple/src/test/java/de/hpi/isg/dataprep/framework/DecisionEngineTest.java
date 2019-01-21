package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.components.DecisionEngine;
import de.hpi.isg.dataprep.preparators.PreparatorTest;
import org.junit.Test;

/**
 * @author lan.jiang
 * @since 1/20/19
 */
public class DecisionEngineTest extends PreparatorTest {

    @Test
    public void selectBestPreparatorTest() {
        // imperfect test. Now the test is done step-by-step in debugging mode.
        DecisionEngine decisionEngine = DecisionEngine.getInstance();
        decisionEngine.selectBestPreparator(dataContext.getDataFrame());
    }
}
