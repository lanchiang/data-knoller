package de.hpi.isg.dataprep.model.target.preparator;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.exceptions.MetadataNotMatchException;
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.error.PropertyError;
import de.hpi.isg.dataprep.model.error.RecordError;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.Target;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.util.Executable;
import de.hpi.isg.dataprep.util.MetadataEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * The abstract class for a {@link Preparator}.
 *
 * @author Lan Jiang
 * @since 2018/6/4
 */
abstract public class Preparator extends Target implements Executable {

    protected List<Metadata> prerequisite; // the prerequisite metadata needed for check before executing the preparator
    protected List<Metadata> toChange; // the metadata that will be changed after executing the preparator

    private Preparation preparation;

    protected List<MetadataEnum> invalid;
    protected Map<String, String> metadataValue;

    protected Dataset<Row> updatedDataset;

    // we do not need such a thing.
    protected PreparatorImpl impl;

    public Preparator() {
        invalid = new ArrayList<>();
        prerequisite = new CopyOnWriteArrayList<>();
        toChange = new CopyOnWriteArrayList<>();
    }

    /**
     * The execution of the preparator.
     *
     */
    protected void executePreparator() throws Exception {
        impl.execute(this);
    }

    /**
     * Call this method whenever an error occurs during the preparator execution in order to
     * record an error log.
     */
    protected void recordErrorLog() {
        Consequences consequences = this.getPreparation().getConsequences();

        List<ErrorLog> errorLogs = consequences.errorsAccumulator_().value().stream()
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

    /**
     * This method validates the input parameters of a {@link Preparator}. If succeeds, setup the values of metadata into both
     * prerequisite and toChange set.
     *
     * @throws Exception
     */
    abstract public void buildMetadataSetup() throws ParameterNotSpecifiedException;

    /**
     * Checks whether the prerequisite metadata are met.
     *
     * @return true/false if all the prerequisiteName are/are not met.
     */
    protected boolean checkMetadata() {
        /**
         * check for each metadata whether valid.
         * Stores invalid ones.
         * If all prerequisiteName are met, read and store all these metadata, used in preparator execution.
         */
        return true;
    }

    /**
     * After the execution of this preparator succeeds, call this method to record the provenance.
     */
    protected void recordProvenance() {

    }

    /**
     * After the execution of this preparator finishes, update the dataset to its intermediate state.
     */
    protected void updateDataset() {
        this.getPreparation().getPipeline().setRawData(this.getUpdatedDataset());
    }

    @Override
    public void execute() throws Exception {
        if (!checkMetadata()) {
            throw new MetadataNotMatchException("Some prerequisite metadata are not met.");
        }
        try {
            executePreparator();
        } catch (PreparationHasErrorException e) {
            recordErrorLog();
        }
        recordProvenance();
        updateDataset();
    }

    public Preparation getPreparation() {
        return preparation;
    }

    public void setPreparation(Preparation preparation) {
        this.preparation = preparation;
    }

    public List<MetadataEnum> getInvalid() {
        return invalid;
    }

    public void setUpdatedDataset(Dataset<Row> updatedDataset) {
        this.updatedDataset = updatedDataset;
    }

    public Dataset<Row> getUpdatedDataset() {
        return updatedDataset;
    }

    public Map<String, String> getMetadataValue() {
        return metadataValue;
    }

    public List<Metadata> getPrerequisite() {
        return prerequisite;
    }

    public List<Metadata> getToChange() {
        return toChange;
    }
}