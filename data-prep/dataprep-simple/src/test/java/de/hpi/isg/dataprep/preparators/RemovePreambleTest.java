package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.RemovePreamble;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Lasse Kohlmeyer
 * @since 2018/11/29
 */
public class RemovePreambleTest extends PreparatorTest {

    @BeforeClass
    public static void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        dialect = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                //.delimiter("\t")
                //x: works
                //.url("./src/test/resources/pokemon.csv")//->X
                //.url("./src/test/resources/restaurants.tsv")//->x
                //.url("./src/test/resources/test.csv")//->x
                //.url("./src/test/resources/test2.csv")//->x
                //.url("./src/test/resources/test21.csv")//->x
                // .url("./src/test/resources/test3.csv")//->x
                //.url("./src/test/resources/test4.csv")//->x
                //.url("./src/test/resources/test5.csv")//->x
                .url("./src/test/resources/test6.csv")//->x
                //.url("./src/test/resources/test7.csv")//->x
                //.url("./src/test/resources/test8.csv")//-> x,
                //.url("./src/test/resources/test9.csv")//->x
                //.url("./src/test/resources/test10.csv")//->x
                //.url("./src/test/resources/test11.csv")//->x
                .buildDialect();

        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
        dataContext = dataLoader.load();

        return;
    }

    //note: different testfilepaths in Preperatortest
    @Test
    public void testRemovePreamble() throws Exception {

        Preparator preparator = new RemovePreamble(super.dialect.getDelimiter(),super.dialect.getHasHeader());

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorRepository trueRepository = new ErrorRepository(trueErrorlogs);

        pipeline.getRawData().show();

        Assert.assertEquals(trueRepository, pipeline.getErrorRepository());
    }

    @Test(expected = ParameterNotSpecifiedException.class)
    public void nullHeaderTestPreamble() throws Exception {

        Preparator preparator = new RemovePreamble(super.dialect.getDelimiter(),null);

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorRepository trueRepository = new ErrorRepository(trueErrorlogs);

        pipeline.getRawData().show();

        Assert.assertEquals(trueRepository, pipeline.getErrorRepository());
    }

    @Test(expected = ParameterNotSpecifiedException.class)
    public void nullDelimiterTestPreamble() throws Exception {

        Preparator preparator = new RemovePreamble(null,super.dialect.getHasHeader());

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
        List<ErrorLog> trueErrorlogs = new ArrayList<>();
        ErrorRepository trueRepository = new ErrorRepository(trueErrorlogs);

        pipeline.getRawData().show();

        Assert.assertEquals(trueRepository, pipeline.getErrorRepository());
    }
}
