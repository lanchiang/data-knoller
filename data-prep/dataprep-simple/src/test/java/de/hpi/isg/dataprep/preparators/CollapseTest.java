package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.preparators.define.Collapse;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/28
 */
public class CollapseTest extends PreparatorTest {

    @Test
    public void testCollapse() throws Exception {
        AbstractPreparator abstractPreparator = new Collapse("identifier");

        Preparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }
}
