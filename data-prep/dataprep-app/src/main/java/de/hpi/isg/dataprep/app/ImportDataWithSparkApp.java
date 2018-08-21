package de.hpi.isg.dataprep.app;

import de.hpi.isg.dataprep.DatasetUtil;
import de.hpi.isg.dataprep.util.DatasetConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Lan Jiang
 * @since 2018/8/6
 */
public class ImportDataWithSparkApp {

    public static void main(String[] args) {
        DatasetConfig datasetConfig = new DatasetConfig();
        datasetConfig.setAppName("Test pipeline");
        datasetConfig.setFilePath("/Users/Fuga/Documents/HPI/data/testdata/pokemon.csv");
        datasetConfig.setMaster(DatasetConfig.Master.local);
        Map<String, String> options = new HashMap<>();
        options.put("header", "true");
        datasetConfig.setOptions(options);
        Dataset<Row> dataset = DatasetUtil.changeFileEncoding(datasetConfig);
        dataset.rdd().saveAsTextFile("/Users/Fuga/Documents/HPI/data/testdata/pokemon_output.csv");
        return;
    }
}
