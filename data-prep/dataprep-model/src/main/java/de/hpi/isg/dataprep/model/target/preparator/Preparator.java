package de.hpi.isg.dataprep.model.target.preparator;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.error.PropertyError;
import de.hpi.isg.dataprep.model.error.RecordError;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.Target;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.util.Executable;
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

    private AbstractPreparation preparation;

    protected List<Metadata> invalid;

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

        List<ErrorLog> errorLogs = consequences.errorsAccumulator().value().stream()
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
    protected void checkMetadata() {
        /**
         * check for each metadata whether valid. Valid metadata are those with correct values.
         * Stores invalid ones.
         * If all prerequisiteName are met, read and store all these metadata, used in preparator execution.
         */
        MetadataRepository metadataRepository = this.getPreparation().getPipeline().getMetadataRepository();

        // but if not match invalid.
        prerequisite.stream()
                .filter(metadata -> metadataRepository.getMetadataPool().contains(metadata)) // but values are not equivalent
                .filter(metadata -> !metadataRepository.equalsByValue(metadata))
                .forEach(metadata -> invalid.add(metadata));
        // not found, add.
        prerequisite.stream()
                .filter(metadata -> !metadataRepository.getMetadataPool().contains(metadata))
                .forEach(metadata -> this.getPreparation().getPipeline().getMetadataRepository().updateMetadata(metadata));
    }

    /**
     * After the execution of this preparator succeeds, call this method to record the provenance.
     */
    private void recordProvenance() {

    }

    private void updateMetadataRepository() {
        MetadataRepository metadataRepository = this.getPreparation().getPipeline().getMetadataRepository();
        metadataRepository.updateMetadata(toChange);
    }

    private void updateDataset() {
        this.getPreparation().getPipeline().setRawData(updatedDataset);
    }

    /**
     * After the execution of this preparator finishes, call this method to post config the pipeline.
     * For example, update the dataset, update the metadata repository.
     */
    private void postConfig() {
        recordProvenance();
        updateMetadataRepository();
        updateDataset();
    }

    /**
     * The template of executing a preparator.
     *
     * @throws Exception
     */
    @Override
    public void execute() throws Exception {
        checkMetadata();
        if (!invalid.isEmpty()) {
            throw new PreparationHasErrorException("Metadata prerequisite not met.");
        }

        try {
            executePreparator();
        } catch (PreparationHasErrorException e) {
            recordErrorLog();
        }
        postConfig();
    }

    public AbstractPreparation getPreparation() {
        return preparation;
    }

    public void setPreparation(AbstractPreparation preparation) {
        this.preparation = preparation;
    }

    public List<Metadata> getInvalid() {
        return invalid;
    }

    public void setUpdatedDataset(Dataset<Row> updatedDataset) {
        this.updatedDataset = updatedDataset;
    }

    public Dataset<Row> getUpdatedDataset() {
        return updatedDataset;
    }

    public List<Metadata> getPrerequisite() {
        return prerequisite;
    }

    public List<Metadata> getToChange() {
        return toChange;
    }
}