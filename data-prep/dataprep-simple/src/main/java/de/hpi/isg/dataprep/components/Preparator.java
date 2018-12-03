package de.hpi.isg.dataprep.components;

import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.error.PropertyError;
import de.hpi.isg.dataprep.model.error.RecordError;
import de.hpi.isg.dataprep.model.repository.MetadataRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 2018/9/17
 */
abstract public class Preparator implements AbstractPreparator {

    protected List<Metadata> prerequisites;
    protected List<Metadata> updates;

    private AbstractPreparation preparation;

    protected List<Metadata> invalid;
    protected Dataset<Row> updatedTable;

    protected PreparatorImpl impl;

    public Preparator() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        invalid = new ArrayList<>();
        prerequisites = new CopyOnWriteArrayList<>();
        updates = new CopyOnWriteArrayList<>();
        impl = newImpl();
    }

    protected abstract PreparatorImpl newImpl();

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
     * The execution of the preparator.
     *
     */
    protected void executePreparator() throws Exception {
        impl.execute(this);
    }

    @Override
    public void checkMetadataPrerequisite() {
        /**
         * check for each metadata whether valid. Valid metadata are those with correct values.
         * Stores invalid ones.
         * If all prerequisiteName are met, read and store all these metadata, used in preparator execution.
         */
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

    @Override
    public void postExecConfig() {
        recordProvenance();
        updateMetadataRepository();
        updateDataset();
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

    @Override
    public List<Metadata> getInvalidMetadata() {
        return invalid;
    }

    @Override
    public List<Metadata> getPrerequisiteMetadata() {
        return prerequisites;
    }

    @Override
    public List<Metadata> getUpdateMetadata() {
        return updates;
    }

    @Override
    public AbstractPreparation getPreparation() {
        return preparation;
    }

    @Override
    public void setUpdatedTable(Dataset<Row> updatedTable) {
        this.updatedTable = updatedTable;
    }

    @Override
    public void setPreparation(AbstractPreparation preparation) {
        this.preparation = preparation;
    }
}
