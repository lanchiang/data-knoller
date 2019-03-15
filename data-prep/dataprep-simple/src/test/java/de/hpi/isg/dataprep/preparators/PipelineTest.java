package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.exceptions.MetadataNotMatchException;
import de.hpi.isg.dataprep.exceptions.PipelineSyntaxErrorException;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PipelineErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.*;
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
 * @since 2018/6/4
 */
public class PipelineTest extends PreparatorTest {

//    @Test
//    public void testPipelineOnRestaurants() throws Exception {
//        pipeline.getDataset().show();
//
//        AbstractPreparator prep1 = new ReplaceSubstring("phone", "/", "-");
//        AbstractPreparation preparation1 = new Preparation(prep1);
//        pipeline.addPreparation(preparation1);
//
//        AbstractPreparator prep2 = new ReplaceSubstring("type", "[(\\s[0-9]+/[0-9]+-[0-9]+\\s)]", "");
//        AbstractPreparation preparation2 = new Preparation(prep2);
//        pipeline.addPreparation(preparation2);
//
//        AbstractPreparator prep3 = new MoveProperty("id", 0);
//        AbstractPreparation preparation3 = new Preparation(prep3);
//        pipeline.addPreparation(preparation3);
//
//        AbstractPreparator prep4 = new MoveProperty("type", 1);
//        AbstractPreparation preparation4 = new Preparation(prep4);
//        pipeline.addPreparation(preparation4);
//
//        AbstractPreparator prep5 = new DeleteProperty("merged_values");
//        AbstractPreparation preparation5 = new Preparation(prep5);
//        pipeline.addPreparation(preparation5);
//
//        AbstractPreparator prep6 = new ChangeDelimiter(pipeline.getDatasetName(), ";");
//        AbstractPreparation preparation6 = new Preparation(prep6);
//        pipeline.addPreparation(preparation6);
//
//        pipeline.executePipeline();
//
//        pipeline.getDataset().show();
//    }

    @Test
    public void testPipelineOnPokemon() throws Exception {

        AbstractPreparator prep1 = new ReplaceSubstring("identifier", "[(\\s)+]", "");
        AbstractPreparation preparation1 = new Preparation(prep1);
        pipeline.addPreparation(preparation1);

        AbstractPreparator prep2 = new ReplaceSubstring("id", "three", "3");
        AbstractPreparation preparation2 = new Preparation(prep2);
        pipeline.addPreparation(preparation2);

        pipeline.executePipeline();
    }

    @Test
    public void testShortPipeline() throws Exception {
        AbstractPreparator abstractPreparator1 = new ChangeDataType("id", DataType.PropertyType.STRING, DataType.PropertyType.INTEGER);
        Preparation preparation1 = new Preparation(abstractPreparator1);
        pipeline.addPreparation(preparation1);

        AbstractPreparator abstractPreparator2 = new ChangeDataType("id", DataType.PropertyType.INTEGER, DataType.PropertyType.STRING);
        Preparation preparation2 = new Preparation(abstractPreparator2);
        pipeline.addPreparation(preparation2);

        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();

        ErrorLog errorLog1 = new PreparationErrorLog(preparation1, "three", new NumberFormatException("For input string: \"three\""));
        ErrorLog errorLog2 = new PreparationErrorLog(preparation1, "six", new NumberFormatException("For input string: \"six\""));
        ErrorLog errorLog3 = new PreparationErrorLog(preparation1, "ten", new NumberFormatException("For input string: \"ten\""));

        trueErrorlogs.add(errorLog1);
        trueErrorlogs.add(errorLog2);
        trueErrorlogs.add(errorLog3);

        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

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
        Assert.assertEquals(updated.count(), 7L);
        Assert.assertEquals(updatedSchema.size(), 12);
    }

    @Test(expected = PipelineSyntaxErrorException.class)
    public void testShortPipelineWithMetadataNotMatch() throws Exception {
        AbstractPreparator abstractPreparator1 = new ChangeDataType("id", DataType.PropertyType.STRING, DataType.PropertyType.INTEGER);
        Preparation preparation1 = new Preparation(abstractPreparator1);
        pipeline.addPreparation(preparation1);

        AbstractPreparator abstractPreparator2 = new ChangeDataType("id", DataType.PropertyType.INTEGER, DataType.PropertyType.STRING);
        Preparation preparation2 = new Preparation(abstractPreparator2);
        pipeline.addPreparation(preparation2);

        AbstractPreparator abstractPreparator3 = new ChangeDataType("id", DataType.PropertyType.INTEGER, DataType.PropertyType.DOUBLE);
        Preparation preparation3 = new Preparation(abstractPreparator3);
        pipeline.addPreparation(preparation3);

        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();

        ErrorLog pipelineError1 = new PipelineErrorLog(pipeline, preparation3,
                new MetadataNotMatchException(String.format("Metadata value does not match that in the repository.")));

        trueErrorlogs.add(pipelineError1);

        ErrorLog errorLog1 = new PreparationErrorLog(preparation1, "three", new NumberFormatException("For input string: \"three\""));
        ErrorLog errorLog2 = new PreparationErrorLog(preparation1, "six", new NumberFormatException("For input string: \"six\""));
        ErrorLog errorLog3 = new PreparationErrorLog(preparation1, "ten", new NumberFormatException("For input string: \"ten\""));

        trueErrorlogs.add(errorLog1);
        trueErrorlogs.add(errorLog2);
        trueErrorlogs.add(errorLog3);

        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());

        Dataset<Row> updated = pipeline.getDataset();
        StructType updatedSchema = updated.schema();

        StructType trueSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("identifier", DataTypes.StringType, true, Metadata.empty()),
                new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("height", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("weight", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("base_experience", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("order", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("is_default", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("stemlemma", DataTypes.StringType, true, Metadata.empty()),
                new StructField("stemlemma2", DataTypes.StringType, true, Metadata.empty()),
                new StructField("stemlemma_wrong", DataTypes.StringType, true, Metadata.empty())
        });

        // Second test whether the schema is correctly updated.
        Assert.assertEquals(trueSchema, updatedSchema);
        Assert.assertEquals(updated.count(), 7L);
        Assert.assertEquals(updatedSchema.size(), 9);
    }
}
