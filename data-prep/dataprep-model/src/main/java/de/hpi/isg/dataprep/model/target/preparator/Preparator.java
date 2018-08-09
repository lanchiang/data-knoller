package de.hpi.isg.dataprep.model.target.preparator;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.model.metadata.PrerequisiteMetadata;
import de.hpi.isg.dataprep.model.target.ErrorLog;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.Preparation;
import de.hpi.isg.dataprep.util.Executable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 2018/6/4
 */
abstract public class Preparator extends AbstractPreparator implements Executable {

    protected PrerequisiteMetadata prerequisites;
    private Preparation preparation;

    protected List<Metadata> invalid;

    protected Dataset<Row> updatedDataset;

    public Preparator() {
        invalid = new ArrayList<>();
    }

    @Override
    protected void recordErrorLog() {
        Consequences consequences = this.getPreparation().getConsequences();

        List<ErrorLog> errorLogs = consequences.errorsAccumulator().value().stream()
                .map(pair -> {
                    String value = pair._1().toString();
                    Throwable exception = pair._2();
                    ErrorLog errorLog = new ErrorLog(value, exception);
                    return errorLog;
                }).collect(Collectors.toList());
        errorLogs.stream()
                .filter(errorLog -> errorLog.getPipelineComponent() == null)
                .forEach(errorLog -> errorLog.setPipelineComponent(this.getPreparation()));
        this.getPreparation().getPipeline().getErrorRepository().addErrorLogs(errorLogs);
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

    public List<Metadata> getInvalid() {
        return invalid;
    }

    public void setUpdatedDataset(Dataset<Row> updatedDataset) {
        this.updatedDataset = updatedDataset;
    }

    public Dataset<Row> getUpdatedDataset() {
        return updatedDataset;
    }
}