package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DataPreparation;
import de.hpi.isg.dataprep.SparkPreparators;
import de.hpi.isg.dataprep.model.target.Errorlog;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.Preparator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashSet;
import java.util.LinkedList;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class ChangeColumnDataType extends Preparator {

    public ChangeColumnDataType() {
        configureMetadataRequisite();
    }

    @Override
    protected void configureMetadataRequisite() {

    }

    @Override
    public void execute(Dataset<Row> dataset) {
        checkMetadataRequisite();
    }

    @Override
    protected void checkMetadataRequisite() {
        this.metadataRequisiteErrorlogs = new LinkedList<Errorlog>();
    }
}
