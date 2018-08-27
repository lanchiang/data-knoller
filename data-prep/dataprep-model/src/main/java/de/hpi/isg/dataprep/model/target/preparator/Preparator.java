package de.hpi.isg.dataprep.model.target.preparator;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.model.error.PropertyError;
import de.hpi.isg.dataprep.model.error.RecordError;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.util.MetadataEnum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
abstract public class Preparator extends AbstractPreparator {

    protected List<Metadata> prerequisiteMetadata;
    protected List<Metadata> toChange;

    private Preparation preparation;

    protected List<MetadataEnum> invalid;
    protected Map<String, String> metadataValue;

    protected Dataset<Row> updatedDataset;

    protected PreparatorImpl impl;

    public Preparator() {
        invalid = new ArrayList<>();
        prerequisiteMetadata = new CopyOnWriteArrayList<>();
        toChange = new CopyOnWriteArrayList<>();
    }

    @Override
    protected void executePreparator() throws Exception {
        impl.execute(this);
    }

    @Override
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

    @Override
    protected boolean checkMetadata() {
        /**
         * check for each metadata whether valid.
         * Stores invalid ones.
         * If all prerequisiteName are met, read and store all these metadata, used in preparator execution.
         */
//        Map<String, String> validMetadata = new HashMap<>();
//        for (MetadataEnum metadata : prerequisiteName) {
//            if (false) {
//                // this metadata is not satisfied, add it to the invalid metadata set.
//                this.getInvalid().add(metadata);
//                return false;
//            }
//            validMetadata.putIfAbsent(metadata.getMetadata(), null);
//        }
//        this.metadataValue = validMetadata;
        return true;
    }

    @Override
    protected void recordProvenance() {

    }

    @Override
    protected void updateDataset() {
        this.getPreparation().getPipeline().setRawData(this.getUpdatedDataset());
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
}