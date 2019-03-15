package de.hpi.isg.dataprep.preparators;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.context.DataContext;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator;
import de.hpi.isg.dataprep.preparators.define.ChangeTableEncoding;
import de.hpi.isg.dataprep.preparators.implementation.EncodingUnmixer;

public class ChangeTableEncodingTest {
    private static final String CSV_DIR = "./src/test/resources/encoding/";
    private static final String NO_ERRORS_URL = CSV_DIR + "no_encoding_error.csv";
    private static final String IN_CSV_URL = CSV_DIR + "error_character.csv";
    private static final String ERRORS_URL = CSV_DIR + "encoding_error.csv";
    private static final String ERRORS_AND_IN_CSV_URL = CSV_DIR + "both_error_and_error_character.csv";
    private static final String MIXED_CSV_URL = CSV_DIR + "telefonbuchalpha_merged.csv";
    private static final String SINGLE_LINE_URL = CSV_DIR + "one_line.csv";
    private static final String NON_EXISTENT_URL = CSV_DIR + "list_of_all_the_friends_i_have.csv";

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

    /*
     * calApplicability tests
     */
    @Test
    public void testNoErrors() {
        DataContext context = load(NO_ERRORS_URL);
        Assert.assertEquals(0, calApplicability(context), 0);
    }

    @Test
    public void testErrorCharsAlreadyInCSV() {
        DataContext context = load(IN_CSV_URL);
        Assert.assertEquals(0, calApplicability(context), 0);
    }

    @Test
    public void testWithErrors() {
        DataContext context = load(ERRORS_URL);
        Assert.assertTrue(calApplicability(context) > 0);
    }

    @Test
    public void testErrorsAndAlreadyInCSV() {
        DataContext context = load(ERRORS_AND_IN_CSV_URL);
        Assert.assertTrue(calApplicability(context) > 0);
    }

    @Test
    public void testMixedCSV() {
        DataContext context = load(MIXED_CSV_URL);
        Assert.assertTrue(calApplicability(context) > 0);
    }

    /*
     * preparator tests
     */
    @Test
    public void testGetCSVPath() {
        DataContext context = load(MIXED_CSV_URL);
        Assert.assertEquals(MIXED_CSV_URL, getCSVPath(context));
    }

    @Test
    public void testGetCurrentEncoding() {
        DataContext context = load(MIXED_CSV_URL);
        Assert.assertEquals("UTF-8", getCurrentEncoding(context));
    }

    /*
     * impl tests
     */
    @Test(expected = FileNotFoundException.class)
    public void testFileNotFoundHandling() throws Exception {
        DataContext context = load(ERRORS_URL);
        Pipeline pipeline = new Pipeline(context);
        pipeline.getMetadataRepository().reset();

        FileLoadDialect newDialect = dialectBuilder.url(NON_EXISTENT_URL).buildDialect();
        pipeline.setDialect(newDialect);

        ChangeTableEncoding preparator = new ChangeTableEncoding();
        pipeline.addPreparation(new Preparation(preparator));
        pipeline.executePipeline();
    }

    /**
     * This test does not pass, because reloading dataset operation complains about "IllegalAccessError: tried to access method com.google.common.base.Stopwatch"
     * @throws Exception
     */
    @Ignore
    @Test
    public void testWrongEncodingInCSV() throws Exception {
        DataContext context = load(ERRORS_URL);
        Pipeline pipeline = new Pipeline(context);

        ChangeTableEncoding preparator = new ChangeTableEncoding();
        pipeline.addPreparation(new Preparation(preparator));
        pipeline.executePipeline();

        Assert.assertEquals("WINDOWS-1252", pipeline.getDialect().getEncoding());
        Assert.assertEquals(0, preparator.calApplicability(null, pipeline.getDataset(), null), 0);
    }

    /*
     * unmixer tests
     */
    @Test
    public void testMixedEncodingInCSV() throws IOException {
        FileLoadDialect dialect = dialectBuilder.url(MIXED_CSV_URL).buildDialect();
        long previousRecordCount = new FlatFileDataLoader(dialect).load().getDataFrame().count();
        FileLoadDialect unmixedDialect = new EncodingUnmixer(MIXED_CSV_URL).unmixEncoding(dialect);

        DataContext context = new FlatFileDataLoader(unmixedDialect).load();
        Pipeline pipeline = new Pipeline(context);
        pipeline.initMetadataRepository();
        ChangeTableEncoding preparator = new ChangeTableEncoding();
        pipeline.addPreparation(new Preparation(preparator));

        try {
            Assert.assertEquals("UTF-8", unmixedDialect.getEncoding());
            Assert.assertEquals(0, preparator.calApplicability(null, pipeline.getDataset(), null), 0);
            Assert.assertEquals(0, preparator.countReplacementChars(unmixedDialect.getUrl()), 0);
            Assert.assertEquals(previousRecordCount, pipeline.getDataset().count());
        } finally {
            Files.delete(Paths.get(unmixedDialect.getUrl()));
        }
    }

    @Test
    public void testSingleLine() throws IOException {
        FileLoadDialect dialect = dialectBuilder.url(SINGLE_LINE_URL).buildDialect();
        FileLoadDialect unmixedDialect = new EncodingUnmixer(SINGLE_LINE_URL).unmixEncoding(dialect);

        DataContext context = new FlatFileDataLoader(unmixedDialect).load();
        Pipeline pipeline = new Pipeline(context);
        pipeline.initMetadataRepository();
        ChangeTableEncoding preparator = new ChangeTableEncoding();
        pipeline.addPreparation(new Preparation(preparator));

        try {
            Assert.assertEquals(0, preparator.calApplicability(null, pipeline.getDataset(), null), 0);
            Assert.assertEquals(0, preparator.countReplacementChars(unmixedDialect.getUrl()), 0);
        } finally {
            Files.delete(Paths.get(unmixedDialect.getUrl()));
        }
    }

    /*
     * helpers
     */
    private float calApplicability(DataContext context) {
        Pipeline pipeline = new Pipeline(context);
        pipeline.initMetadataRepository();

        AbstractPreparator preparator = new ChangeTableEncoding();
        pipeline.addPreparation(new Preparation(preparator));
        return preparator.calApplicability(null, pipeline.getDataset(), null);
    }

    private String getCSVPath(DataContext context) {
        Pipeline pipeline = new Pipeline(context);
        pipeline.initMetadataRepository();

        ChangeTableEncoding preparator = new ChangeTableEncoding();
        pipeline.addPreparation(new Preparation(preparator));
        return preparator.getCsvPath().get();
    }

    private String getCurrentEncoding(DataContext context) {
        Pipeline pipeline = new Pipeline(context);
        pipeline.initMetadataRepository();

        ChangeTableEncoding preparator = new ChangeTableEncoding();
        pipeline.addPreparation(new Preparation(preparator));
        return preparator.getCurrentEncoding().get();
    }

    private DataContext load(String url) {
        FileLoadDialect dialect = dialectBuilder.url(url).buildDialect();
        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
        return dataLoader.load();
    }
}
