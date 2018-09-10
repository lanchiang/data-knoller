package de.hpi.isg.dataprep.loader;

import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import org.apache.spark.sql.Dataset;
import org.junit.Test;

/**
 * @author Lan Jiang
 * @since 2018/9/7
 */
public class testDataLoader {

    @Test
    public void testLoadData() {
        SparkDataLoader dataLoader = new FlatFileDataLoader("Test", "local");
        dataLoader.setUrl("./src/test/resources/pokemon.csv");
        dataLoader.addOption("header", "true").addOption("inferSchema", "true");
        dataLoader.load();

        Dataset<?> dataset = dataLoader.getDataContext().getDataset();
        dataset.show();
        dataset.printSchema();
    }
}
