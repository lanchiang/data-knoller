package de.hpi.isg.dataprep.model;

import de.hpi.isg.dataprep.model.target.Pipeline;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class PipelineTest {

    private Dataset<Row> dataset;

    @Before
    public void setUp() throws Exception {
        dataset = SparkSession.builder()
                .appName("Pipeline test")
                .master("local")
                .getOrCreate()
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("/Users/Fuga/Documents/HPI/data/testdata/pokemon.csv");
    }

    @Test
    public void testCreatePipeline() throws Exception {
        Pipeline pipeline = new Pipeline(dataset);
        Preparator preparator = new ChangePropertyDataType("id", ChangePropertyDataType.PropertyType.INTEGER);
        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
    }

}
