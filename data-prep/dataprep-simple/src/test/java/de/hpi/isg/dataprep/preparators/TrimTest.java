package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.config.DataLoadingConfig;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.preparators.define.Trim;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class TrimTest extends DataLoadingConfig {

    @BeforeClass
    public static void setUp() {
//        transforms = DataLoadingConfig.createTransformsManually();
        DataLoadingConfig.basicSetup();
    }
//
//    @Test
//    public void testTrim() throws Exception {
//        AbstractPreparator abstractPreparator = new Trim("identifier");
//
//        AbstractPreparation preparation = new Preparation(abstractPreparator);
//        pipeline.addPreparation(preparation);
//        pipeline.executePipeline();
//
//        List<ErrorLog> trueErrorlogs = new ArrayList<>();
//        ErrorRepository trueRepository = new ErrorRepository(trueErrorlogs);
//        Assert.assertEquals(trueRepository, pipeline.getErrorRepository());
//    }
//
    @Test(expected = RuntimeException.class)
    public void testTrimNonexistColumn() throws Exception {
        AbstractPreparator preparator = new Trim("nonexist");
        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
    }
}
