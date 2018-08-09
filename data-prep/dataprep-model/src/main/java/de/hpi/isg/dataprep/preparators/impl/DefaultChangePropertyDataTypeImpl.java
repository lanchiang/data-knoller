package de.hpi.isg.dataprep.preparators.impl;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType;
import de.hpi.isg.dataprep.spark.ChangePropertyDataTypeScala;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Lan Jiang
 * @since 2018/8/9
 */
public class DefaultChangePropertyDataTypeImpl extends ChangePropertyDataTypeImpl {

    @Override
    public void execute(ChangePropertyDataType preparator) throws Exception {
        Dataset<Row> dataset = preparator.getPreparation().getPipeline().getRawData();

        Consequences consequences = ChangePropertyDataTypeScala.changePropertyDataType(dataset, preparator);
        preparator.getPreparation().setConsequences(consequences);
        if (consequences.hasError()) {
            throw new PreparationHasErrorException("This preparation causes errors for some records.");
        }

        preparator.setUpdatedDataset(dataset);
    }
}
