package de.hpi.isg.dataprep.framework;

import de.hpi.isg.dataprep.components.DecisionEngine;
import de.hpi.isg.dataprep.config.DataLoadingConfig;
import de.hpi.isg.dataprep.model.target.schema.Attribute;
import de.hpi.isg.dataprep.model.target.schema.Transform;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.ChangeDateFormat;
import de.hpi.isg.dataprep.preparators.define.DeleteProperty;
import de.hpi.isg.dataprep.preparators.define.SplitProperty;
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl;
import de.hpi.isg.dataprep.schema.transforms.TransDeleteAttribute;
import de.hpi.isg.dataprep.schema.transforms.TransSplitAttribute;
import de.hpi.isg.dataprep.util.DatePattern;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lan.jiang
 * @since 1/20/19
 */
public class DecisionEngineTest extends DataLoadingConfig {

    private final DecisionEngine decisionEngine = DecisionEngine.getInstance();

    @BeforeClass
    public static void setUp() {
        resourcePath = "./src/test/resources/decisionEngine.csv";
        transforms = DecisionEngineTest.createTransformsManually();
        DataLoadingConfig.setUp();
    }

    @Test
    public void selectBestPreparatorTest() {
        decisionEngine.setPreparatorCandidates(new String[]{
                "AddProperty", "Collapse", "DeleteProperty", "Hash"
        });
        AbstractPreparator actualPreparator = decisionEngine.selectBestPreparator(pipeline);
        AbstractPreparator expectedPreparator = new DeleteProperty("date");

        Assert.assertEquals(expectedPreparator, actualPreparator);
    }

    @Test
    public void selectSplitPropertyTest() {
        decisionEngine.setPreparatorCandidates(new String[]{"SplitProperty"});
        AbstractPreparator actualPreparator = decisionEngine.selectBestPreparator(pipeline);
        AbstractPreparator expectedPreparator = new SplitProperty("date_split", new DefaultSplitPropertyImpl.SingleValueStringSeparator("-"), 3, true);

        Assert.assertEquals(expectedPreparator, actualPreparator);
    }

    @Test
    public void selectBetweenDeletePropertyAndSplitPropertyTest() {
        decisionEngine.setPreparatorCandidates(new String[]{
                "DeleteProperty", "SplitProperty"
        });
        AbstractPreparator actualPreparator = decisionEngine.selectBestPreparator(pipeline);
        AbstractPreparator expectedPreparator = new SplitProperty("date_split", new DefaultSplitPropertyImpl.SingleValueStringSeparator("-"), 3, true);

        Assert.assertEquals(expectedPreparator, actualPreparator);
    }

    @Test
    public void selectChangeDateFormatTest() {
        decisionEngine.setPreparatorCandidates(new String[]{"ChangeDateFormat"});
        AbstractPreparator actualPreparator = decisionEngine.selectBestPreparator(pipeline);
        AbstractPreparator expectedPreparator = new ChangeDateFormat("date_format", Option.apply(null), Option.apply(DatePattern.DatePatternEnum.DayMonthYear));

        Assert.assertEquals(expectedPreparator, actualPreparator);
    }

    protected static List<Transform> createTransformsManually() {
        // generate schema mapping
        List<Transform> transforms = new ArrayList<>();
        Attribute sourceAttribute = new Attribute(new StructField("date", DataTypes.StringType, true, emptyMetadata));
        Transform deleteAttr = new TransDeleteAttribute(sourceAttribute);
        transforms.add(deleteAttr);

        sourceAttribute = new Attribute(new StructField("date_split", DataTypes.StringType, true, emptyMetadata));
        Attribute[] targetAttrs = new Attribute[3];
        targetAttrs[0] = new Attribute(new StructField("date_split_1", DataTypes.StringType, true, emptyMetadata));
        targetAttrs[1] = new Attribute(new StructField("date_split_2", DataTypes.StringType, true, emptyMetadata));
        targetAttrs[2] = new Attribute(new StructField("date_split_3", DataTypes.StringType, true, emptyMetadata));
        Transform splitAttr = new TransSplitAttribute(sourceAttribute, targetAttrs);
        transforms.add(splitAttr);
//
        return transforms;
    }
}
