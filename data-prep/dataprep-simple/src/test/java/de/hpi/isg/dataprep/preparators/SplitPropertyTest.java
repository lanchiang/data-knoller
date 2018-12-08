package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.Padding;
import de.hpi.isg.dataprep.preparators.define.SplitProperty;
import org.apache.commons.lang.ArrayUtils;
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
 * @author Jakob Köhler
 * @since 2018/11/30
 */
public class SplitPropertyTest extends PreparatorTest {

    private StructType newSchema = new StructType(new StructField[] {
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
            new StructField("date1", DataTypes.StringType, true, Metadata.empty()),
            new StructField("date2", DataTypes.StringType, true, Metadata.empty()),
            new StructField("date3", DataTypes.StringType, true, Metadata.empty()),
    });

    @Test
    public void testSplitPropertyWithAllParameters() throws Exception {
        Preparator preparator = new SplitProperty("date", "-", 3, true);
        assertTest(preparator);
    }

    @Test
    public void testSplitPropertyWithPropertyAndSeparator() throws Exception {
        Preparator preparator = new SplitProperty("date", "-");
        assertTest(preparator);
    }

    @Test
    public void testSplitPropertyWithOnlyProperty() throws Exception {
        Preparator preparator = new SplitProperty("date");
        assertTest(preparator);
    }

    private void assertTest(Preparator preparator) throws Exception {
        pipeline.addPreparation(new Preparation(preparator));
        pipeline.executePipeline();

        Assert.assertEquals(new ErrorRepository(new ArrayList<>()), pipeline.getErrorRepository());

        Assert.assertEquals(newSchema, pipeline.getRawData().schema());

        pipeline.getRawData().foreach(
                row -> {
                    String[] expectedSplit = row.get(8).toString().split("-");
                    String[] split = {row.get(12).toString(), row.get(13).toString(), row.get(14).toString()};
                    if(expectedSplit.length == 1) {
                        String[] singleValueSplit = {expectedSplit[0], "", ""};
                        Assert.assertArrayEquals(singleValueSplit, split);
                    } else {
                        Assert.assertArrayEquals(expectedSplit, split);
                    }
                }
        );
    }

    @Test
    public void testSplitPropertyWithFromLeftIsFalse() throws Exception {
        Preparator preparator = new SplitProperty("date", "-", 3, false);
        pipeline.addPreparation(new Preparation(preparator));
        pipeline.executePipeline();

        Assert.assertEquals(new ErrorRepository(new ArrayList<>()), pipeline.getErrorRepository());

        Assert.assertEquals(newSchema, pipeline.getRawData().schema());

        pipeline.getRawData().foreach(
                row -> {
                    String[] expectedSplit = row.get(8).toString().split("-");
                    ArrayUtils.reverse(expectedSplit); // reverse expectedSplit
                    String[] split = {row.get(12).toString(), row.get(13).toString(), row.get(14).toString()};
                    if(expectedSplit.length == 1) {
                        String[] singleValueSplit = {expectedSplit[0], "", ""};
                        Assert.assertArrayEquals(singleValueSplit, split);
                    } else {
                        Assert.assertArrayEquals(expectedSplit, split);
                    }
                }
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgumentExceptionWhenNumColsIs1() throws Exception {
        Preparator preparator = new SplitProperty("date", "-", 1, true);
        pipeline.addPreparation(new Preparation(preparator));
        pipeline.executePipeline();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgumentExceptionWhenPropertyNameDoNotExist() throws Exception {
        Preparator preparator = new SplitProperty("xyz", "-", 3, true);
        pipeline.addPreparation(new Preparation(preparator));
        pipeline.executePipeline();
    }
}