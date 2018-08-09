package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.metadata.ChangePropertyDataTypeMetadata;
import de.hpi.isg.dataprep.model.target.Metadata;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.spark.ChangePropertyDataTypeImpl;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author Lan Jiang
 * @since 2018/8/7
 */
public class ChangePropertyDataType extends Preparator {

    public enum PropertyType {
        STRING,
        INTEGER,
        DOUBLE,
        DATE,
        DATETIME,
        DATETIMESTAMP
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

        Consequences consequences = ChangePropertyDataTypeImpl.changePropertyDataType(dataset, propertyName, targetType);
        this.getPreparation().setConsequences(consequences);
        if (consequences.hasError()) {
            throw new PreparationHasErrorException("This preparation causes errors for some records.");
        }

        this.setUpdatedDataset(dataset);
    }

    @Override
    protected void recordProvenance() {

    }
}
