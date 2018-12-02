package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.ExplodeArray;
import de.hpi.isg.dataprep.preparators.define.SplitAttribute;
import org.junit.Test;

public class SplitPreparatorTest {

    @Test
    public void testTrivialSplit() throws Exception {
        Preparator preperator = new SplitAttribute("_c0");
        Preparator prep1 = new ExplodeArray("_c0");

        FileLoadDialect dialect = new DialectBuilder()
                .url("./src/test/resources/splitAttributeTest.csv")
                .delimiter("~")
                .buildDialect();

        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
        DataContext dataContext = dataLoader.load();
        AbstractPipeline pipeline = new Pipeline(dataContext);

        dataContext.getDataFrame().show();

        AbstractPreparation preparation = new Preparation(preperator);
        AbstractPreparation preparation1 = new Preparation(prep1);
        pipeline.addPreparation(preparation);
        pipeline.addPreparation(preparation1);
        pipeline.executePipeline();

        pipeline.getRawData().show();

    }


}
