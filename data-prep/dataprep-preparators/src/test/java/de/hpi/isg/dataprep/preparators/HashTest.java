package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.system.Preparation;
import de.hpi.isg.dataprep.util.HashAlgorithm;
import org.junit.Test;

/**
 * @author Lan Jiang
 * @since 2018/9/4
 */
public class HashTest extends PreparatorTest {

    @Test
    public void testHashMD5() throws Exception {
        Preparator preparator = new Hash("order", HashAlgorithm.MD5);

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
    }

    @Test
    public void testHashSHA() throws Exception {
        Preparator preparator = new Hash("species_id", HashAlgorithm.SHA);

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();
    }
}
