package de.hpi.isg.dataprep.implementation;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.model.error.PreparationError;
import de.hpi.isg.dataprep.model.error.PropertyError;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.preparators.AddProperty;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;

/**
 * @author Lan Jiang
 * @since 2018/8/20
 */
abstract public class AddPropertyImpl extends PreparatorImpl {

    abstract protected Consequences executeLogic(AddProperty preparator,
                                                 Dataset<Row> dataFrame,
                                                 CollectionAccumulator<PreparationError> errorAccumulator);

    @Override
    protected Consequences executePreparator(Preparator preparator, Dataset<Row> dataFrame) throws Exception {
        AddProperty preparator_ = this.getPreparatorInstance(preparator, AddProperty.class);
        CollectionAccumulator<PreparationError> errorAccumulator = this.createErrorAccumulator(preparator, dataFrame);
        return this.executeLogic(preparator_, dataFrame, errorAccumulator);
    }

    @Override
    protected CollectionAccumulator<PreparationError> createErrorAccumulator(Preparator preparator, Dataset<Row> dataFrame) {
        CollectionAccumulator<PreparationError> errorAccumulator = new CollectionAccumulator<>();
        dataFrame.sparkSession().sparkContext().register(errorAccumulator,
                String.format("%s error accumulator.", ((AddProperty)preparator)).getClass().getSimpleName());
        return errorAccumulator;
    }
}
