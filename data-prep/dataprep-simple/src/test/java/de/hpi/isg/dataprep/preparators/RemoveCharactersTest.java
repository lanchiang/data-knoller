package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.preparators.define.RemoveCharacters;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.util.RemoveCharactersMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/9/4
 */
public class RemoveCharactersTest extends PreparatorTest {

    @Test
    public void testRemoveNumeric() throws Exception {
        Preparator preparator = new RemoveCharacters("id", RemoveCharactersMode.NUMERIC);

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testRemoveNonAlphanumeric() throws Exception {
        Preparator preparator = new RemoveCharacters("base_experience", RemoveCharactersMode.NONALPHANUMERIC);

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testRemoveCustom() throws Exception {
        Preparator preparator = new RemoveCharacters("id", RemoveCharactersMode.CUSTOM, "ee");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }
}
