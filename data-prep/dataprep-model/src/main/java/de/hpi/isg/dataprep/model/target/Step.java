package de.hpi.isg.dataprep.model.target;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
public class Step extends Target implements Serializable {

    private Preparator preparator;
    private List<Errorlog> errorlogs;
    private Dataset<Row> dataset;

    public Step(Preparator preparator) {
        this.preparator = preparator;
        errorlogs = new LinkedList<Errorlog>();
    }

    public void setDataset(Dataset<Row> dataset) {
        this.dataset = dataset;
    }

    public void executePreparator() {
        this.preparator.execute();
    }
}
