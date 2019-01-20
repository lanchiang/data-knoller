package de.hpi.isg.dataprep.model.target.system;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl;
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.error.PropertyError;
import de.hpi.isg.dataprep.model.error.RecordError;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.data.ColumnCombination;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping;
import de.hpi.isg.dataprep.util.Executable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * This interface defines all the common behaviors of every preparator.
 *
 * @author Lan Jiang
 * @since 2018/9/17
 */
abstract public class AbstractPreparator implements Executable {

    protected List<Metadata> prerequisites;
    protected List<Metadata> updates;

    private AbstractPreparation preparation;

    protected List<Metadata> invalid;
    protected Dataset<Row> updatedTable;

    /**
     * This data structure stores the applicability score of this preparator on each {@link ColumnCombination}.
     */
    protected Map<ColumnCombination, Float> applicability;

    protected AbstractPreparatorImpl impl;

    public AbstractPreparator() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        invalid = new ArrayList<>();
        prerequisites = new CopyOnWriteArrayList<>();
        updates = new CopyOnWriteArrayList<>();

        applicability = new HashMap<>();

//        impl = newImpl();

        String simpleClassName = this.getClass().getSimpleName();

        String preparatorImplClass = "de.hpi.isg.dataprep.preparators.implementation." + "Default" + simpleClassName + "Impl";
        this.impl = Class.forName(preparatorImplClass).asSubclass(AbstractPreparatorImpl.class).newInstance();
    }

    /**
     * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
     * prerequisite and toChange set.
     *
     * @throws ParameterNotSpecifiedException
     */
    // TODO: Maybe set it protectected? Check Pipeline implementation @Gerardo
    abstract public void buildMetadataSetup() throws ParameterNotSpecifiedException;

    @Override
    public void execute() throws Exception {
        checkMetadataPrerequisite();
        if (!invalid.isEmpty()) {
            throw new PreparationHasErrorException("Metadata prerequisite not met.");
        }

        try {
            executePreparator();
        } catch (PreparationHasErrorException e) {
            recordErrorLog();
        }
        postExecConfig();

    }

    /**
     * Calculate the applicability score of the preparator on the given data slice. The caller of
     * this function provide it with a data slice (a subset of the columns or the rows of the data).
     * This method calculate the score of applying this preparator only on this data slice. The caller may
     * possible call this function many times to calculate the scores for all different combinations of
     * the data slices. For example, for the preparator SplitAttribute, the data slice represents a single
     * column, and the caller will call this method for each column in the data.
     *
     * @param schemaMapping is the schema of the input data towards the schema of the output data.
     * @param dataset is the input dataset slice. A slice can be a subset of the columns of the data,
     *                or a subset of the rows of the data.
     * @param targetMetadata is the set of {@link Metadata} that shall be fulfilled for the output data
     * @return the applicability matrix succinctly represented by a hash map. Each key stands for
     *  a {@link ColumnCombination} in the dataset, and its value the applicability score of this preparator signature.
     */
    abstract public float calApplicability(SchemaMapping schemaMapping, Dataset<Row> dataset, Collection<Metadata> targetMetadata);

    /**
     * Return a new parameter-free preparator instance.
     *
     * @param clazz specifies the concrete preparator that should be returned.
     * @return the parameter-free concrete preparator.
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static AbstractPreparator getPreparatorInstance(Class<? extends AbstractPreparator> clazz)
            throws IllegalAccessException, InstantiationException {
        AbstractPreparator preparator = clazz.newInstance();
        return preparator;
    }

    /**
     * The execution of the preparator.
     */
    protected void executePreparator() throws Exception {
        impl.execute(this);
    }

    /**
     * This method checks whether values of the prerequisite metadata are met. It shall preserve the unsatisfying metadata.
     */
    public void checkMetadataPrerequisite() {
        /**
         * check for each metadata whether valid. Valid metadata are those with correct values.
         * Stores invalid ones.
         * If all prerequisiteName are met, read and store all these metadata, used in preparator execution.
         */

        //Added dependency on model.metadata repository @Gerardo
        MetadataRepository metadataRepository = this.getPreparation().getPipeline().getMetadataRepository();

        // but if not match invalid.
        prerequisites.stream()
                .forEach(metadata -> {
                    Metadata that = metadataRepository.getMetadata(metadata);
                    if (that == null || !metadata.equalsByValue(that)) {
                        invalid.add(metadata);
                    }
                });
        // not found, add.
//        prerequisite.stream()
//                .filter(metadata -> !metadataRepository.getMetadataPool().contains(metadata))
//                .forEach(metadata -> this.getPreparation().getPipeline().getMetadataRepository().updateMetadata(metadata));
    }

    /**
     * Call this method whenever an error occurs during the preparator execution in order to
     * record an error log.
     */
    private void recordErrorLog() {
        ExecutionContext executionContext = this.getPreparation().getExecutionContext();

        List<ErrorLog> errorLogs = executionContext.errorsAccumulator().value().stream()
                .map(error -> {
                    ErrorLog errorLog = null;
                    switch (error.getErrorLevel()) {
                        case RECORD: {
                            RecordError pair = (RecordError) error;
                            String value = pair.getErrRecord();
                            Throwable exception = pair.getError();
                            errorLog = new PreparationErrorLog(this.getPreparation(), value, exception);
                            break;
                        }
                        case PROPERTY: {
                            PropertyError pair = (PropertyError) error;
                            String value = pair.getProperty();
                            Throwable throwable = pair.getThrowable();
                            errorLog = new PreparationErrorLog(this.getPreparation(), value, throwable);
                            break;
                        }
                        case DATASET: {
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                    return errorLog;
                }).collect(Collectors.toList());
        this.getPreparation().getPipeline().getErrorRepository().addErrorLogs(errorLogs);
    }

    private void recordProvenance() {

    }

    private void updateMetadataRepository() {
        MetadataRepository metadataRepository = this.getPreparation().getPipeline().getMetadataRepository();
        metadataRepository.updateMetadata(updates);
    }

    private void updateDataset() {
        this.getPreparation().getPipeline().setRawData(updatedTable);
    }

    /**
     * After the execution of this preparator finishes, call this method to post config the pipeline.
     * For example, update the dataset, update the metadata repository.
     */

    public void postExecConfig() {
        recordProvenance();
        updateMetadataRepository();
        updateDataset();
    }

    public Map<ColumnCombination, Float> getApplicability() {
        return applicability;
    }

    public List<Metadata> getInvalidMetadata() {
        return invalid;
    }

    public List<Metadata> getPrerequisiteMetadata() {
        return prerequisites;
    }

    public List<Metadata> getUpdateMetadata() {
        return updates;
    }

    public AbstractPreparation getPreparation() {
        return preparation;
    }

    public void setUpdatedTable(Dataset<Row> updatedTable) {
        this.updatedTable = updatedTable;
    }

    public void setPreparation(AbstractPreparation preparation) {
        this.preparation = preparation;
    }

}
