package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.preparators.define.DeleteProperty;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/20
 */
public class DeletePropertyTest extends PreparatorTest {

    @Test
    public void testDeleteExistingProperty() throws Exception {
        AbstractPreparator abstractPreparator = new DeleteProperty("species_id");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorLog = new ArrayList<>();
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorLog);

        // First test error log repository
        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());

        Dataset<Row> updated = pipeline.getDataset();
        StructType updatedSchema = updated.schema();

        StructType trueSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("identifier", DataTypes.StringType, true, Metadata.empty()),
                new StructField("height", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("weight", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("base_experience", DataTypes.StringType, true, Metadata.empty()),
                new StructField("order", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("is_default", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("stemlemma", DataTypes.StringType, true, Metadata.empty()),
                new StructField("stemlemma2", DataTypes.StringType, true, Metadata.empty()),
                new StructField("stemlemma_wrong", DataTypes.StringType, true, Metadata.empty()),
        });

        // Second test whether the schema is correctly updated.
        Assert.assertEquals(trueSchema, updatedSchema);
    }

    @Test
    public void testDeleteNonExistingProperty() throws Exception {
        AbstractPreparator abstractPreparator = new DeleteProperty("nonexist");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorLog = new ArrayList<>();
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorLog);
        ErrorLog errorLog = new PreparationErrorLog(preparation, "nonexist", new PreparationHasErrorException("The property to be deleted does not exist."));
        trueErrorLog.add(errorLog);

        // First test error log repository
        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());

        Dataset<Row> updated = pipeline.getDataset();
        StructType updatedSchema = updated.schema();

        StructType trueSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("identifier", DataTypes.StringType, true, Metadata.empty()),
                new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("height", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("weight", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("base_experience", DataTypes.StringType, true, Metadata.empty()),
                new StructField("order", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("is_default", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("stemlemma", DataTypes.StringType, true, Metadata.empty()),
                new StructField("stemlemma2", DataTypes.StringType, true, Metadata.empty()),
                new StructField("stemlemma_wrong", DataTypes.StringType, true, Metadata.empty()),
        });

        // Second test whether the schema is correctly updated.
        Assert.assertEquals(trueSchema, updatedSchema);
    }
}
