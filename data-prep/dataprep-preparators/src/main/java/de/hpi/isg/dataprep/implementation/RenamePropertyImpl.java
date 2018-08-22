package de.hpi.isg.dataprep.implementation;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.model.error.PreparationError;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.preparators.RenameProperty;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;

/**
 * @author Lan Jiang
 * @since 2018/8/17
 */
abstract public class RenamePropertyImpl extends PreparatorImpl {

    abstract protected Consequences executeLogic(RenameProperty preparator,
                                                 Dataset<Row> dataFrame,
                                                 CollectionAccumulator<PreparationError> errorAccumulator);

    @Override
    protected final Consequences executePreparator(Preparator preparator, Dataset<Row> dataFrame) throws Exception {
        RenameProperty preparator_ = this.getPreparatorInstance(preparator, RenameProperty.class);
        CollectionAccumulator<PreparationError> errorAccumulator = this.createErrorAccumulator(preparator_, dataFrame);
        return this.executeLogic(preparator_, dataFrame, errorAccumulator);
    }

    @Override
    protected final CollectionAccumulator<PreparationError> createErrorAccumulator(Preparator preparator, Dataset<Row> dataFrame) {
        CollectionAccumulator<PreparationError> errorAccumulator = new CollectionAccumulator<>();
        dataFrame.sparkSession().sparkContext().register(errorAccumulator,
                String.format("%s error acumulator", RenameProperty.class.getSimpleName()));
        return errorAccumulator;
    }
}
