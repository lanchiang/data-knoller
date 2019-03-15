package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.preparators.define.Trim;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
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
        AbstractPreparator abstractPreparator = new Trim("identifier");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();

//        ErrorLog pipelineError = new PipelineErrorLog(pipeline,
//                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
//                        "propertyName='" + "identifier" + '\'' +
//                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
//                        '}')));
//
//        trueErrorlogs.add(pipelineError);
        ErrorRepository trueRepository = new ErrorRepository(trueErrorlogs);

//        pipeline.getDataset().show();

        Assert.assertEquals(trueRepository, pipeline.getErrorRepository());
    }
}
