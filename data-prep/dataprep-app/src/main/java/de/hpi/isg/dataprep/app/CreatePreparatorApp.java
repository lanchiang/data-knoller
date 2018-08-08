package de.hpi.isg.dataprep.app;

import de.hpi.isg.dataprep.model.target.Pipeline;
import de.hpi.isg.dataprep.model.target.Preparation;
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
        Pipeline pipeline = new Pipeline(rawData);
        Preparator preparator = new ChangeFileEncoding("UTF-8");
        Preparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
    }
}
