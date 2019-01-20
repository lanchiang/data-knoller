package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.ChangeTableEncoding;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ChangeTableEncodingTest extends PreparatorTest {
    private static final String CSV_DIR = "./src/test/resources/encoding/";
    private static final String NO_ERRORS_URL = CSV_DIR + "no_encoding_error.csv";
    private static final String IN_CSV_URL = CSV_DIR + "error_character.csv";
    private static final String ERRORS_URL = CSV_DIR + "encoding_error.csv";
    private static final String ERRORS_AND_IN_CSV_URL = CSV_DIR + "both_error_and_error_character.csv";

    private static final String ENCODING = "UTF-8";
    private static DialectBuilder dialectBuilder;

    @BeforeClass
    public static void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        dialectBuilder = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                .encoding(ENCODING);
    }

    // calApplicability

    @Test
    public void testNoErrors() throws Exception {
        Pipeline pipeline = load(NO_ERRORS_URL);
        Assert.assertEquals(0, calApplicability(pipeline), 0);
    }

    @Test
    public void testErrorCharsAlreadyInCSV() {
        Pipeline pipeline = load(IN_CSV_URL);
        Assert.assertEquals(0, calApplicability(pipeline), 0);
    }

    @Test
    public void testWithErrors() {
        Pipeline pipeline = load(ERRORS_URL);
        Assert.assertTrue(calApplicability(pipeline) > 0);
    }

    @Test
    public void testErrorsAndAlreadyInCSV() {
        Pipeline pipeline = load(ERRORS_AND_IN_CSV_URL);
        Assert.assertTrue(calApplicability(pipeline) > 0);
    }

    private float calApplicability(Pipeline pipeline) {
        AbstractPreparator preparator = new ChangeTableEncoding();
        return preparator.calApplicability(null, pipeline.getRawData(), null);
    }

    private Pipeline load(String url) {
        FileLoadDialect dialect = dialectBuilder.url(url).buildDialect();
        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
        DataContext context = dataLoader.load();

        Pipeline pipeline = new Pipeline(context);
        pipeline.initMetadataRepository();
        return pipeline;
    }

    @Override
    public void cleanUpPipeline() {}
}
