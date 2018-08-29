package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.implementation.defaults.DefaultPaddingImpl;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.DataType;
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
        Preparator preparator = new Padding(new DefaultPaddingImpl());
        ((Padding) preparator).setPropertyName("id");
        ((Padding) preparator).setExpectedLength(8);

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorLog pipelineError1 = new PipelineErrorLog(pipeline,
                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
                        "propertyName='" + "id" + '\'' +
                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
                        '}')));
        trueErrorlogs.add(pipelineError1);
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        pipeline.getRawData().show();

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testPaddingValueTooLong() throws Exception {
        Preparator preparator = new Padding(new DefaultPaddingImpl());
        ((Padding) preparator).setPropertyName("id");
        ((Padding) preparator).setExpectedLength(4);
        ((Padding) preparator).setPadder("x");

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorLog pipelineError1 = new PipelineErrorLog(pipeline,
                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
                        "propertyName='" + "id" + '\'' +
                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
                        '}')));
        trueErrorlogs.add(pipelineError1);

        ErrorLog errorlog1 = new PreparationErrorLog(preparation, "three",
                new IllegalArgumentException(String.format("Value length is already larger than padded length.")));
        trueErrorlogs.add(errorlog1);

        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        pipeline.getRawData().show();

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }
}
