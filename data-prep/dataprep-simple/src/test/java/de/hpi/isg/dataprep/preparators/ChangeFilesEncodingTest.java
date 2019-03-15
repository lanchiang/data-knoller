package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.ExecutionContext;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.objects.Metadata;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.ChangeFilesEncoding;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ChangeFilesEncodingTest extends PreparatorTest {
    private static final String TEST_DATA_PATH = "./src/test/resources/digimon.csv";

    private static final String PROPERTY_NAME = "bio_path";  // property containing the paths to test data
    private static final String OLD_ENCODING = "cp1252";     // actual encoding of test data
    private static final String NEW_ENCODING = "UTF-8";      // target encoding for tests

    private static String[] oldPaths;  // paths to test data, these files should not be deleted during cleanup

    @BeforeClass
    public static void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        FileLoadDialect dialect = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                .url(TEST_DATA_PATH)
                .buildDialect();
        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
        dataContext = dataLoader.load();

        // populate oldPaths
        try {
            pipeline = new Pipeline(dataContext);
            pipeline.executePipeline();
            oldPaths = getPaths();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // delete files created during tests
    @After
    public void cleanUp() {
        String[] newPaths = getPaths();
        for (String path : newPaths) {
            if (!Arrays.asList(oldPaths).contains(path)) {
                if (!new File(path).delete())
                    Assert.fail("Warning: files created by this test could not be deleted, file path=" + path);
            }
        }
    }

    /* Test happy path */

    @Test
    public void testChangeKnownEncoding() throws Exception {
        ChangeFilesEncoding preparator = new ChangeFilesEncoding(PROPERTY_NAME, OLD_ENCODING, NEW_ENCODING);
        testWorkingPreparator(preparator, Charset.forName(OLD_ENCODING), Charset.forName(NEW_ENCODING));
    }

    @Test
    public void testChangeUnknownEncoding() throws Exception {
        ChangeFilesEncoding preparator = new ChangeFilesEncoding(PROPERTY_NAME, NEW_ENCODING);
        testWorkingPreparator(preparator, Charset.forName(OLD_ENCODING), Charset.forName(NEW_ENCODING));
    }

    // ************************** Important bug to be fixed. Now suspended. **************************
    // The preparator should use the source encoding from the metadata before trying to detect it
    // Since we set the wrong encoding, the conversion should fail
    // (if he hadn't set it, the preparator would detect the correct encoding and succeed)
//    @Test
//    public void testSourceEncodingFromMetadata() throws Exception {
//        Metadata fakeMetadata = new FileEncoding(PROPERTY_NAME, "ASCII");
//        ChangeEncoding preparator = new MockChangeEncoding(PROPERTY_NAME, NEW_ENCODING, fakeMetadata);
//        executePreparator(preparator);
//        assertErrorCount((int) pipeline.getDataset().count());
//    }


    /* Test I/O errors */

    @Test
    public void testFileNotFound() throws Exception {
        Dataset<Row> oldData = pipeline.getDataset();
        Dataset<Row> newData = oldData.withColumn(PROPERTY_NAME, functions.lit("not a real path"));
        pipeline.setDataset(newData);

        ChangeFilesEncoding preparator = new ChangeFilesEncoding(PROPERTY_NAME, OLD_ENCODING, NEW_ENCODING);
        executePreparator(preparator);
        assertErrorCount((int) newData.count());
        pipeline.setDataset(oldData);  // restore actual paths so cleanUp doesn't complain
    }

    @Test
    public void testWrongSourceEncoding() throws Exception {
        String oldEncoding = "ASCII";
        ChangeFilesEncoding preparator = new ChangeFilesEncoding(PROPERTY_NAME, oldEncoding, NEW_ENCODING);
        executePreparator(preparator);
        assertErrorCount((int) pipeline.getDataset().count());
    }

    @Test
    public void testTargetEncodingCannotEncodeSource() throws Exception {
        String newEncoding = "ASCII";
        ChangeFilesEncoding preparator = new ChangeFilesEncoding(PROPERTY_NAME, OLD_ENCODING, newEncoding);
        executePreparator(preparator);
        assertErrorCount((int) pipeline.getDataset().count());
    }


    /* Test malformed parameters */

    @Test(expected = ParameterNotSpecifiedException.class)
    public void testNullProperty() throws Exception {
        ChangeFilesEncoding preparator = new ChangeFilesEncoding(null, NEW_ENCODING);
        executePreparator(preparator);
    }

    @Test(expected = ParameterNotSpecifiedException.class)
    public void testNullDestEnc() throws Exception {
        ChangeFilesEncoding preparator = new ChangeFilesEncoding(PROPERTY_NAME, OLD_ENCODING, null);
        executePreparator(preparator);
    }

    @Test(expected = IllegalCharsetNameException.class)
    public void testInvalidSourceEnc() throws Exception {
        String oldEncoding = "not a real encoding";
        ChangeFilesEncoding preparator = new ChangeFilesEncoding(PROPERTY_NAME, oldEncoding, NEW_ENCODING);
        executePreparator(preparator);
    }

    @Test(expected = IllegalCharsetNameException.class)
    public void testInvalidDestEnc() throws Exception {
        String newEncoding = "not a real encoding";
        ChangeFilesEncoding preparator = new ChangeFilesEncoding(PROPERTY_NAME, newEncoding);
        executePreparator(preparator);
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidProperty() throws Exception {
        ChangeFilesEncoding preparator = new ChangeFilesEncoding("fake property", NEW_ENCODING);
        executePreparator(preparator);
    }


    /* Helpers */

    private void testWorkingPreparator(ChangeFilesEncoding preparator, Charset oldCharset, Charset newCharset) throws Exception {
        executePreparator(preparator);
        assertErrorCount(0);

        String[] newPaths = getPaths();
        for (int i = 0; i < oldPaths.length; i++) {
            Assert.assertNotEquals(oldPaths[i], newPaths[i]);
            Assert.assertEquals(readFile(oldPaths[i], oldCharset), readFile(newPaths[i], newCharset));
        }
    }

    // The error repository is too convoluted to recreate, just count the errors and pray to god it's the right ones
    private void assertErrorCount(int errorCount) {
        Assert.assertEquals(errorCount, pipeline.getErrorRepository().getErrorLogs().size());
    }

    private void executePreparator(ChangeFilesEncoding preparator) throws Exception {
        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
    }

    private String readFile(String path, Charset encoding) throws FileNotFoundException {
        InputStreamReader isr = new InputStreamReader(new FileInputStream(path), encoding);
        return new Scanner(isr).useDelimiter("\\A").next();
    }

    private static String[] getPaths() {
        return pipeline.getDataset()
                .select(PROPERTY_NAME)
                .collectAsList()
                .stream()
                .map(row -> row.getString(0))
                .toArray(String[]::new);
    }

    // This class adds fake metadata before executing the preparator
    private static class MockChangeFilesEncoding extends ChangeFilesEncoding {
        private Metadata fakeMetadata;

        private MockChangeFilesEncoding(String propertyName, String targetEncoding, Metadata fakeMetadata) {
            super(propertyName, targetEncoding);
            this.fakeMetadata = fakeMetadata;
        }

        @Override
        public ExecutionContext execute(Dataset<Row> dataset) throws Exception {
            this.getPreparation().getPipeline().getMetadataRepository().updateMetadata(fakeMetadata);
            return super.execute(dataset);
        }
    }
}
