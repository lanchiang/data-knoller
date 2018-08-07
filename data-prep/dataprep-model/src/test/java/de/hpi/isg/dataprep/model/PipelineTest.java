package de.hpi.isg.dataprep.model;

import de.hpi.isg.dataprep.DatasetUtil;
import de.hpi.isg.dataprep.model.target.Pipeline;
import de.hpi.isg.dataprep.util.DatasetConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class PipelineTest {

    @Test
    public void testCreatePipeline() {
        DatasetConfig datasetConfig = new DatasetConfig();
        datasetConfig.setAppName("Test pipeline");
        datasetConfig.setFilePath("/Users/Fuga/Documents/HPI/data/testdata/pokemon.csv");
        datasetConfig.setInputFileFormat(DatasetConfig.InputFileFormat.CSV);
        datasetConfig.setMaster(DatasetConfig.Master.local);
        Map<String, String> options = new HashMap<>();
        options.put("header", "true");
        datasetConfig.setOptions(options);
        Dataset<Row> dataset = DatasetUtil.createDataset(datasetConfig);
        Pipeline pipeline = new Pipeline(dataset);
//        Preparator preparator = new RenameColumn("height","高度");
//        pipeline.addPreparator(preparator);
//        Preparator preparator1 = new ChangeColumnDataType("id",
//                ChangeColumnDataType.ColumnDataType.Decimal);
//        pipeline.addPreparator(preparator1);
//        pipeline.executePipeline();
    }

}
