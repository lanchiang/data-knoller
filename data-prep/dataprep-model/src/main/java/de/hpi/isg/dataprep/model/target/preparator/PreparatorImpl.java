package de.hpi.isg.dataprep.model.target.preparator;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Lan Jiang
 * @since 2018/8/18
 */
abstract public class PreparatorImpl {

    abstract protected Consequences executePreparator(Preparator preparator, Dataset<Row> dataFrame) throws Exception;

    public void execute(Preparator preparator) throws Exception {
        Dataset<Row> dataset = preparator.getPreparation().getPipeline().getRawData();

        Consequences consequences = executePreparator(preparator, dataset);

        preparator.getPreparation().setConsequences(consequences);
        preparator.setUpdatedDataset(consequences.newDataFrame_());

        if (consequences.hasError()) {
            throw new PreparationHasErrorException("This preparation causes errors for some records.");
        }
    }

}
