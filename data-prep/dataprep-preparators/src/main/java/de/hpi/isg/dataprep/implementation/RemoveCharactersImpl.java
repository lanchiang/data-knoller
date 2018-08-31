package de.hpi.isg.dataprep.implementation;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.model.error.PreparationError;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.preparators.RemoveCharacters;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;

/**
 * @author Lan Jiang
 * @since 2018/8/30
 */
abstract public class RemoveCharactersImpl extends PreparatorImpl {

    abstract protected Consequences executeLogic(RemoveCharacters preparator,
                                                 Dataset<Row> dataFrame,
                                                 CollectionAccumulator<PreparationError> errorAccumulator);

    @Override
    protected Consequences executePreparator(Preparator preparator, Dataset<Row> dataFrame) throws Exception {
        RemoveCharacters preparator_ = this.getPreparatorInstance(preparator, RemoveCharacters.class);
        CollectionAccumulator<PreparationError> errorAccumulator = this.createErrorAccumulator(dataFrame);
        return this.executeLogic(preparator_, dataFrame, errorAccumulator);
    }
}
