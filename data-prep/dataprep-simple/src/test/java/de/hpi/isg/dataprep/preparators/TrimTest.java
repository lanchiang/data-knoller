package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.util.DataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class TrimTest extends PreparatorTest {

    @Test
    public void testTrim() throws Exception {
        Preparator preparator = new Trim("identifier");

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();

        ErrorLog pipelineError = new PipelineErrorLog(pipeline,
                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
                        "propertyName='" + "identifier" + '\'' +
                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
                        '}')));

        trueErrorlogs.add(pipelineError);
        ErrorRepository trueRepository = new ErrorRepository(trueErrorlogs);

        pipeline.getRawData().show();

        Assert.assertEquals(trueRepository, pipeline.getErrorRepository());
    }
}
