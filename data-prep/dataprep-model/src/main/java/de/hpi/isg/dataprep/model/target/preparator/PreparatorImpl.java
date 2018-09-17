package de.hpi.isg.dataprep.model.target.preparator;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.error.PreparationError;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;

/**
 * @author Lan Jiang
 * @since 2018/8/18
 */
abstract public class PreparatorImpl {

    abstract protected Consequences executePreparator(AbstractPreparator preparator, Dataset<Row> dataFrame) throws Exception;

    protected final CollectionAccumulator<PreparationError> createErrorAccumulator(Dataset<Row> dataFrame) {
        CollectionAccumulator<PreparationError> errorAccumulator = new CollectionAccumulator();
        dataFrame.sparkSession().sparkContext().register(errorAccumulator,
                String.format("Error accumulator registered."));
        return errorAccumulator;
    }

    private final Dataset<Row> getDataSet(AbstractPreparator preparator) {
        return preparator.getPreparation().getPipeline().getRawData();
    }

    public final void execute(AbstractPreparator preparator) throws Exception {
        // getDataset
        Dataset<Row> dataset = this.getDataSet(preparator);

        Consequences consequences = executePreparator(preparator, dataset);

        preparator.getPreparation().setConsequences(consequences);
        preparator.setUpdatedTable(consequences.newDataFrame());

        if (consequences.hasError()) {
            throw new PreparationHasErrorException("This preparation causes errors for some records.");
        }
    }

    public final <T> T getPreparatorInstance(AbstractPreparator preparator, Class<T> concretePreparatorClass) throws ClassCastException {
        if (!(preparator.getClass().isAssignableFrom(concretePreparatorClass))) {
            throw new ClassCastException("Class is not required type.");
        }
        T realPreparator = (T) preparator;
        return realPreparator;
    }

}
