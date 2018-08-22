package de.hpi.isg.dataprep.implementation;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.model.error.PreparationError;
import de.hpi.isg.dataprep.model.error.RecordError;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.preparators.ChangePropertyDataType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;

/**
 * The delegation class for {@link ChangePropertyDataType}. Maybe not needed.
 *
 * @author Lan Jiang
 * @since 2018/8/9
 */
abstract public class ChangePropertyDataTypeImpl extends PreparatorImpl {

    abstract protected Consequences executeLogic(ChangePropertyDataType preparator,
                                                 Dataset<Row> dataFrame,
                                                 CollectionAccumulator<PreparationError> errorAccumulator);

    @Override
    protected final Consequences executePreparator(Preparator preparator, Dataset<Row> dataFrame) throws Exception {
        ChangePropertyDataType preparator_ = this.getPreparatorInstance(preparator, ChangePropertyDataType.class);
        CollectionAccumulator<PreparationError> errorAccumulator = this.createErrorAccumulator(preparator_, dataFrame);

        return executeLogic(preparator_, dataFrame, errorAccumulator);
    }

    @Override
    protected final CollectionAccumulator<PreparationError> createErrorAccumulator(Preparator preparator, Dataset<Row> dataFrame) {
        CollectionAccumulator<PreparationError> errorAccumulator = new CollectionAccumulator();
        dataFrame.sparkSession().sparkContext().register(errorAccumulator,
                String.format("%s error accumulator.", ChangePropertyDataType.class.getSimpleName()));
        return errorAccumulator;
    }
}