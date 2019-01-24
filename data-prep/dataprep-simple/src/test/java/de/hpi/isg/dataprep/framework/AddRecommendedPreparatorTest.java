package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.PreparatorTest;
import de.hpi.isg.dataprep.preparators.define.AddProperty;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author lan.jiang
 * @since 1/20/19
 */
public class AddRecommendedPreparatorTest extends PreparatorTest {

    @Ignore
    @Test
    public void addRecommendedPreparationTest() {
        while (true) {
            boolean succeed = pipeline.addRecommendedPreparation();
            if (!succeed) {
                break;
            }
        }
    }
}
