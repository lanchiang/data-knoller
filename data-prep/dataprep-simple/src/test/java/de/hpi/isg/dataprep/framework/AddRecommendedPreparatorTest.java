package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.config.DataLoadingConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test on adding a recommended preparation to the pipeline and execute it.
 *
 * @author lan.jiang
 * @since 1/20/19
 */
public class AddRecommendedPreparatorTest extends DataLoadingConfig {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void addRecommendedPreparationTest() {
//        expectedException.expect(RuntimeException.class);
//        expectedException.expectMessage("Internal error. Decision engine fails to select the best preparator.");

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
//        pipeline.print();
    }
}
