package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.preparators.define.AddProperty;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.util.DataType;
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
public class AddPropertyTest extends PreparatorTest {

    @Test
    public void testAddIntegerColumn() throws Exception {
        AbstractPreparator abstractPreparator = new AddProperty("classic", DataType.PropertyType.INTEGER, 4, 8);

        Preparation preparation = new Preparation(abstractPreparator);
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
                new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("height", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("classic", DataTypes.IntegerType, false, Metadata.empty()),
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
    public void testAddDoubleColumn() throws Exception {
        AbstractPreparator abstractPreparator = new AddProperty("price", DataType.PropertyType.DOUBLE, 2);

        Preparation preparation = new Preparation(abstractPreparator);
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
                new StructField("price", DataTypes.DoubleType, false, Metadata.empty()),
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

    @Test
    public void testAddStringColumn() throws Exception {
        AbstractPreparator abstractPreparator = new AddProperty("name", DataType.PropertyType.STRING, 7);

        Preparation preparation = new Preparation(abstractPreparator);
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
                new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("height", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("weight", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("base_experience", DataTypes.StringType, true, Metadata.empty()),
                new StructField("order", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
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
    public void testAddDateColumn() throws Exception {
        AbstractPreparator abstractPreparator = new AddProperty("ship", DataType.PropertyType.DATE, 5);

        Preparation preparation = new Preparation(abstractPreparator);
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
                new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("height", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("weight", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ship", DataTypes.DateType, true, Metadata.empty()),
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
    public void testAddColumnOutOfBound() throws Exception {
        AbstractPreparator abstractPreparator = new AddProperty("ship", DataType.PropertyType.DATE, 20);

        Preparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorLog = new ArrayList<>();
        ErrorLog errorLog = new PreparationErrorLog(preparation, "ship",
                new PreparationHasErrorException(String.format("Position %d is out of bound.", 20)));
        trueErrorLog.add(errorLog);
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorLog);

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
