package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.components.DecisionEngine;
import de.hpi.isg.dataprep.config.DatasetsLoadingConfig;
import de.hpi.isg.dataprep.preparators.define.SuggestableRemovePreamble;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;

/**
 * @author Lan Jiang
 * @since 2019-04-12
 */
public class SuggestPreparatorTest extends DatasetsLoadingConfig {

    private final DecisionEngine decisionEngine = DecisionEngine.getInstance();

    @Ignore
    @Test
    public void selectBestPreparatorTest() throws URISyntaxException {
        File[] preambled_files = new File(getClass().getResource("/preambled").toURI().getPath()).listFiles();

        for (File preambled_file : preambled_files) {
            resourcePath = preambled_file.getPath();
            super.setUp();

            decisionEngine.setPreparatorCandidates(new String[]{
                    "SuggestRemovePreamble"
            });

            AbstractPreparator actualPreparator = decisionEngine.selectBestPreparator(pipeline);
            AbstractPreparator expectedPreparator = new SuggestableRemovePreamble(null);

            Assert.assertEquals(expectedPreparator, actualPreparator);
        }
    }
}
