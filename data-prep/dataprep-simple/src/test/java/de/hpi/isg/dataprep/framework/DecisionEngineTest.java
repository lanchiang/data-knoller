package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.components.DecisionEngine;
import de.hpi.isg.dataprep.config.DataLoadingConfig;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.DeleteProperty;
import org.junit.Assert;
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

        decisionEngine.printPreparatorCandidates();

        decisionEngine.setPreparatorCandidates(new String[]{
                "AddProperty", "Collapse", "DeleteProperty", "Hash"
        });

        AbstractPreparator actualPreparator = decisionEngine.selectBestPreparator(pipeline);

        AbstractPreparator expectedPreparator = new DeleteProperty("date");

        Assert.assertEquals(expectedPreparator, actualPreparator);
    }
}
