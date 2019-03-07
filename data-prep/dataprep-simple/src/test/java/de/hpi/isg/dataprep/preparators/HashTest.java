package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.preparators.define.Hash;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.util.HashAlgorithm;
import org.junit.Test;

/**
 * @author Lan Jiang
 * @since 2018/9/4
 */
public class HashTest extends PreparatorTest {

    @Test
    public void testHashMD5() throws Exception {
        AbstractPreparator abstractPreparator = new Hash("order", HashAlgorithm.MD5);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
    }

    @Test
    public void testHashSHA() throws Exception {
        AbstractPreparator abstractPreparator = new Hash("species_id", HashAlgorithm.SHA);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
    }
}
