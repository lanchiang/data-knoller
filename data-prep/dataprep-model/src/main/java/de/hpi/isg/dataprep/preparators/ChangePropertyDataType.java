package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.DatasetUtil;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.metadata.ChangePropertyDataTypeMetadata;
import de.hpi.isg.dataprep.model.target.ErrorLog;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.spark.ChangePropertyDataTypeImpl;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 2018/8/7
 */
public class ChangePropertyDataType extends Preparator {

    public enum PropertyType {
        String,
        Integer,
        Double
    }

    private String propertyName;
    private PropertyType targetType;

    public ChangePropertyDataType(String propertyName, PropertyType targetType) {
        this.propertyName = propertyName;
        this.targetType = targetType;
    }

    public ChangePropertyDataType(String propertyName, PropertyType sourceType, PropertyType targetType) {
        this(propertyName, targetType);
    }

    @Override
    protected boolean checkMetadata() {
        prerequisites = new ChangePropertyDataTypeMetadata();
        for (String metadata : prerequisites.getPrerequisites()) {
            if (false) {
                // this metadata is not satisfied, add it to the invalid metadata set.
                this.getInvalid().add(new Metadata(metadata));
                return false;
            }
        }
        return true;
    }

    @Override
    protected void executePreparator() throws Exception {
        Dataset<Row> dataset = this.getPreparation().getPipeline().getRawData();

        Consequences consequences = ChangePropertyDataTypeImpl.changePropertyDataType(dataset, "id", PropertyType.Integer);
        this.getPreparation().setConsequences(consequences);
        if (consequences.hasError()) {
            throw new PreparationHasErrorException("This preparation causes errors for some records.");
        }

        this.setUpdatedDataset(dataset);
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
                .filter(errorLog -> errorLog.getPreparation() == null)
                .forEach(errorLog -> errorLog.setPreparation(this.getPreparation()));
        this.getPreparation().getPipeline().getErrorRepository().addErrorLogs(errorLogs);
    }

    @Override
    protected void recordProvenance() {

    }
}
