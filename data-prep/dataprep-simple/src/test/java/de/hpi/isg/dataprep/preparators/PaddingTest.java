package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.config.DataLoadingConfig;
import de.hpi.isg.dataprep.exceptions.PreparationHasErrorException;
import de.hpi.isg.dataprep.io.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.io.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
        ;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.preparators.define.Padding;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.errorlog.PreparationErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lan Jiang
 * @since 2018/8/29
 */
public class PaddingTest extends DataLoadingConfig {

    @BeforeClass
    public static void setUp() {
        resourcePath = "./src/test/resources/pokemon.csv";

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        dialect = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                .url(resourcePath)
                .buildDialect();

        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
        dataContext = dataLoader.load();
    }

    @Test
    public void testPaddingAllShorterValue() throws Exception {
        AbstractPreparator abstractPreparator = new Padding("id", 8);

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
    }

    @Test
    public void testPaddingValueTooLong() throws Exception {
        AbstractPreparator abstractPreparator = new Padding("id", 4, "x");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();

        ErrorLog errorlog1 = new PreparationErrorLog(preparation, "three",
                new IllegalArgumentException(String.format("Value length is already larger than padded length.")));
        trueErrorlogs.add(errorlog1);

        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
        Assert.assertEquals(pipeline.getDataset().count(), 9L);
    }

    @Test(expected = RuntimeException.class)
    public void testPaddingNotExistProperty() throws Exception {
        AbstractPreparator abstractPreparator = new Padding("notExist", 4, "x");

        AbstractPreparation preparation = new Preparation(abstractPreparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        List<ErrorLog> trueErrorlogs = new ArrayList<>();

        ErrorLog errorlog1 = new PreparationErrorLog(preparation, "three",
                new IllegalArgumentException(String.format("Value length is already larger than padded length.")));
        trueErrorlogs.add(errorlog1);

        ErrorRepository trueErrorRepository = new ErrorRepository(trueErrorlogs);

        Assert.assertEquals(trueErrorRepository, pipeline.getErrorRepository());
        Assert.assertEquals(pipeline.getDataset().count(), 9L);
    }
}
