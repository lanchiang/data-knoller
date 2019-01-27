package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.preparators.PreparatorTest;
import org.junit.Test;

/**
 * Test on adding a recommended preparation to the pipeline and execute it.
 *
 * @author lan.jiang
 * @since 1/20/19
 */
public class AddRecommendedPreparatorTest extends PreparatorTest {

    @Test
    public void addRecommendedPreparationTest() {
        /**
         * Iteratively adding a new preparation to the pipeline. The preparation added by the method 'addRecommendedPreparation', which is
         * selected by the decision engine will be executed immediately after added. When the decision engine decide to terminate the
         * process, it tells 'addRecommendedPreparation' to return false, and therefore this loop ends.
         */
        while (true) {
            boolean succeed = pipeline.addRecommendedPreparation();
            if (!succeed) {
                break;
            }
        }

        // print the constructed pipeline to the console
        pipeline.print();
    }
}
