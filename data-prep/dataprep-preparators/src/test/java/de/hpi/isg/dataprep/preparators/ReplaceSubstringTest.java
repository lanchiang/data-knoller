package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.exceptions.MetadataNotFoundException;
import de.hpi.isg.dataprep.implementation.defaults.DefaultReplaceSubstringImpl;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.util.DataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/30
 */
public class ReplaceSubstringTest extends PreparatorTest {

    @Test
    public void testReplaceAllNormalString() throws Exception {
        Preparator preparator = new ReplaceSubstring(new DefaultReplaceSubstringImpl());
        ((ReplaceSubstring) preparator).setPropertyName("identifier");
        ((ReplaceSubstring) preparator).setSource("cha");
        ((ReplaceSubstring) preparator).setReplacement("CT");

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorLog pipelineErrorLog = new PipelineErrorLog(pipeline,
                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
                        "propertyName='" + "identifier" + '\'' +
                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
                        '}')));
        errorLogs.add(pipelineErrorLog);
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        pipeline.getRawData().show();

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testReplaceFirstSomeNormalString() throws Exception {
        Preparator preparator = new ReplaceSubstring(new DefaultReplaceSubstringImpl());
        ((ReplaceSubstring) preparator).setPropertyName("identifier");
        ((ReplaceSubstring) preparator).setSource("b");
        ((ReplaceSubstring) preparator).setReplacement("mam");
        ((ReplaceSubstring) preparator).setFirstSome(1);

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorLog pipelineErrorLog = new PipelineErrorLog(pipeline,
                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
                        "propertyName='" + "identifier" + '\'' +
                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
                        '}')));
        errorLogs.add(pipelineErrorLog);
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        pipeline.getRawData().show();

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testReplaceAllWithRegex() throws Exception {
        Preparator preparator = new ReplaceSubstring(new DefaultReplaceSubstringImpl());
        ((ReplaceSubstring) preparator).setPropertyName("identifier");
        ((ReplaceSubstring) preparator).setSource("(\\s+)([a-z]+)(\\s+)");
        ((ReplaceSubstring) preparator).setReplacement("REP");

        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorLog pipelineErrorLog = new PipelineErrorLog(pipeline,
                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
                        "propertyName='" + "identifier" + '\'' +
                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
                        '}')));
        errorLogs.add(pipelineErrorLog);
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        pipeline.getRawData().show();

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
    }

//    @Test
//    public void testFirstSomeParameterIllegal() throws Exception {
//        Preparator preparator = new ReplaceSubstring(new DefaultReplaceSubstringImpl());
//        ((ReplaceSubstring) preparator).setPropertyName("identifier");
//        ((ReplaceSubstring) preparator).setSource("(\\s+)([a-z]+)(\\s+)");
//        ((ReplaceSubstring) preparator).setReplacement("REP");
//        ((ReplaceSubstring) preparator).setFirstSome(-1);
//
//        Preparation preparation = new Preparation(preparator);
//        pipeline.addPreparation(preparation);
//        pipeline.executePipeline();
//
//        List<ErrorLog> errorLogs = new ArrayList<>();
//        ErrorLog pipelineErrorLog = new PipelineErrorLog(pipeline,
//                new MetadataNotFoundException(String.format("The metadata %s not found in the repository.", "PropertyDataType{" +
//                        "propertyName='" + "identifier" + '\'' +
//                        ", propertyDataType=" + DataType.PropertyType.STRING.toString() +
//                        '}')));
//        errorLogs.add(pipelineErrorLog);
//        ErrorRepository errorRepository = new ErrorRepository(errorLogs);
//
//        pipeline.getRawData().show();
//
//        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());
//    }
}
