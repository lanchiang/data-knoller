package de.hpi.isg.dataprep.components;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.error.PreparationError;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;

/**
 * @author Lan Jiang
 * @since 2018/8/18
 */
abstract public class AbstractPreparatorImpl {

//    abstract protected ExecutionContext executePreparator(AbstractPreparator preparator, Dataset<Row> dataFrame) throws Exception;

    protected final ExecutionContext executePreparator(AbstractPreparator preparator, Dataset<Row> dataFrame) throws Exception {
        CollectionAccumulator<PreparationError> errorAccumulator = this.createErrorAccumulator(dataFrame);
        return executeLogic(preparator, dataFrame, errorAccumulator);
    }

    /**
     * The abstract class of preparator implementation.
     *
     * @param abstractPreparator is the instance of {@link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
     * @param dataFrame          contains the intermediate dataset
     * @param errorAccumulator   is the {@link CollectionAccumulator} to store preparation errors while executing the preparator.
     * @return an instance of {@link ExecutionContext} that includes the new dataset, and produced errors.
     * @throws Exception
     */
    abstract protected ExecutionContext executeLogic(AbstractPreparator abstractPreparator,
                                                     Dataset<Row> dataFrame,
                                                     CollectionAccumulator<PreparationError> errorAccumulator) throws Exception;

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

        ExecutionContext executionContext = executePreparator(preparator, dataset);

        preparator.getPreparation().setExecutionContext(executionContext);
        preparator.setUpdatedTable(executionContext.newDataFrame());

        // throw the runtime errors to the preparator. The preparator is due to record the errors.
        if (executionContext.hasError()) {
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
