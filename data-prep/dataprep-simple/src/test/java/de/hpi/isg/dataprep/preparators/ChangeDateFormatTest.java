package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.ChangeDateFormat;
import de.hpi.isg.dataprep.preparators.define.ReplaceSubstring;
import de.hpi.isg.dataprep.util.DatePattern;
import org.junit.Assert;
import org.junit.Test;
import scala.Option;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/30
 */
public class ChangeDateFormatTest extends PreparatorTest {

    @Test
    public void testReplaceAllNormalString() throws Exception {
        Preparator preparator = new ChangeDateFormat("identifier",
                Option.apply(DatePattern.DatePatternEnum.DayMonthYear),
                DatePattern.DatePatternEnum.YearDayMonth);

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        pipeline.getRawData().show();

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }
}
