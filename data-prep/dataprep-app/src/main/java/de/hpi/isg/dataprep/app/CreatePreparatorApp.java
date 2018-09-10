package de.hpi.isg.dataprep.app;

import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.preparators.ChangeFileEncoding;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Lan Jiang
 * @since 2018/8/6
 */
public class CreatePreparatorApp {

    private static Dataset<Row> rawData = null;

    public static void main(String[] args) throws Exception {
        PipelineOld pipeline = new PipelineOld(rawData);
        Preparator preparator = new ChangeFileEncoding("UTF-8");
        PreparationOld preparation = new PreparationOld(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
    }
}
