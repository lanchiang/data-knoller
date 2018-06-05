package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.SparkPreparators;
import de.hpi.isg.dataprep.model.target.Preparator;
import de.hpi.isg.dataprep.parameter.Parameter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class RenameColumn extends Preparator {

    public RenameColumn(Parameter parameters) {
        configureMetadataRequisite();
        super.parameters = parameters;
    }

    @Override
    protected void configureMetadataRequisite() {

    }

    @Override
    protected void checkMetadataRequisite() {

    }

    @Override
    public void execute(Dataset<Row> dataset) {
        checkMetadataRequisite();
        Dataset<Row> ds = SparkPreparators.renameColumn(dataset, "height", "高度");
        ds.show();
    }
}
