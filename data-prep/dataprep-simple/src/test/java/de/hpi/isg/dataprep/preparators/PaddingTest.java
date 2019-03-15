package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.preparators.define.Padding;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class PaddingTest extends PreparatorTest {

    @Test
    public void testPaddingAllShorterValue() throws Exception {
        AbstractPreparator abstractPreparator = new Padding("id", 8);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
//        ErrorLog pipelineError1 = new PipelineErrorLog(pipeline,
//                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
//                        "propertyName='" + "id" + '\'' +
//                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
//                        '}')));
//        trueErrorlogs.add(pipelineError1);
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testPaddingValueTooLong() throws Exception {
        AbstractPreparator abstractPreparator = new Padding("id", 4, "x");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
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

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
        Assert.assertEquals(pipeline.getDataset().count(), 9L);
    }
}
