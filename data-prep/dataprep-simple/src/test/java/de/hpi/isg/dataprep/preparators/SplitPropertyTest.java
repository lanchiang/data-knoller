package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.Padding;
import de.hpi.isg.dataprep.preparators.define.SplitProperty;
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

    @Test
    public void testSplitProperty() throws Exception {
        Preparator preparator = new SplitProperty("date");

        pipeline.addPreparation(new Preparation(preparator));
        pipeline.executePipeline();

        Assert.assertEquals(new ErrorRepository(new ArrayList<>()), pipeline.getErrorRepository());

        StructType newSchema = new StructType(new StructField[] {
                new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("identifier", DataTypes.StringType, true, Metadata.empty()),
                new StructField("species_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("height", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("weight", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("base_experience", DataTypes.StringType, true, Metadata.empty()),
                new StructField("order", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("is_default", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("date1", DataTypes.StringType, true, Metadata.empty()),
                new StructField("date2", DataTypes.StringType, true, Metadata.empty()),
                new StructField("date3", DataTypes.StringType, true, Metadata.empty())
        });
        Assert.assertEquals(newSchema, pipeline.getRawData().schema());

        pipeline.getRawData().foreach(
                row -> {
                    String[] expectedSplit = row.get(8).toString().split("-");
                    String[] split = {row.get(9).toString(), row.get(10).toString(), row.get(11).toString()};
                    if(expectedSplit.length == 1) {
                        String[] singleValueSplit = {expectedSplit[0], "", ""};
                        Assert.assertArrayEquals(singleValueSplit, split);
                    } else {
                        Assert.assertArrayEquals(expectedSplit, split);
                    }
                }
        );
    }
}
