package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.Padding;
import de.hpi.isg.dataprep.preparators.define.SplitProperty;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class SplitPropertyTest extends PreparatorTest {

    @Test
    public void testPaddingAllShorterValue() throws Exception {
        Preparator preparator = new SplitProperty("date");

        pipeline.addPreparation(new Preparation(preparator));
        pipeline.executePipeline();
        pipeline.getRawData().show();

        Assert.assertEquals(new ErrorRepository(new ArrayList<>()), pipeline.getErrorRepository());
    }

    @Test
    public void testPaddingValueTooLong() throws Exception {
        Preparator preparator = new Padding("id", 4, "x");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
//        ErrorLog pipelineError1 = new PipelineErrorLog(pipeline,
//                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
//                        "propertyName='" + "id" + '\'' +
//                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
//                        '}')));
//        trueErrorlogs.add(pipelineError1);

        ErrorLog errorlog1 = new PreparationErrorLog(preparation, "three",
                new IllegalArgumentException(String.format("Value length is already larger than padded length.")));
        trueErrorlogs.add(errorlog1);

        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        pipeline.getRawData().show();

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
        Assert.assertEquals(pipeline.getRawData().count(), 9L);
    }
}
