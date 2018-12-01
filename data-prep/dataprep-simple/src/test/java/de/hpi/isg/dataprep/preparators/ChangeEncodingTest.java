package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.exceptions.EncodingNotSupportedException;
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.ChangeEncoding;
import de.hpi.isg.dataprep.util.ChangeEncodingMode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Lan Jiang
 * @since 2018/9/4
 */
public class ChangeEncodingTest extends PreparatorTest {
    private static String[] newPaths = new String[0];
    private static String[] oldPaths;

    @BeforeClass
    public static void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        FileLoadDialect dialect = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                .url("./src/test/resources/digimon.csv")
                .buildDialect();

        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
        dataContext = dataLoader.load();

        try {
            pipeline = new Pipeline(dataContext);
            pipeline.executePipeline();
            oldPaths = getPaths();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @After
    public void cleanUp() {
        for (String path : newPaths) {
            if (!Arrays.asList(oldPaths).contains(path)) new File(path).delete();
        }
        newPaths = new String[0];
    }
    /* Test happy path */

    @Test
    public void testChangeKnownEncoding() throws Exception {
        String oldEncoding = "cp1252";
        String newEncoding = "UTF-8";
        ChangeEncoding preparator = new ChangeEncoding("bio_path", ChangeEncodingMode.SOURCEANDTARGET, oldEncoding, newEncoding);
        testWorkingPreparator(preparator, Charset.forName(oldEncoding), Charset.forName(newEncoding));
    }

    @Test
    public void testChangeUnknownEncoding() throws Exception {
        String oldEncoding = "cp1252";
        String newEncoding = "UTF-8";
        ChangeEncoding preparator = new ChangeEncoding("bio_path", ChangeEncodingMode.GIVENTARGET, newEncoding);
        testWorkingPreparator(preparator, Charset.forName(oldEncoding), Charset.forName(newEncoding));
    }


    /* Test I/O errors */

    @Test
    public void testFileNotFound() throws Exception {
        Dataset<Row> oldData = pipeline.getRawData();
        Dataset<Row> newdata = oldData.withColumn("bio_path", functions.lit("fake_path"));
        pipeline.setRawData(newdata);

        String oldEncoding = "cp1252";
        String newEncoding = "UTF-8";
        ChangeEncoding preparator = new ChangeEncoding("bio_path", ChangeEncodingMode.SOURCEANDTARGET, oldEncoding, newEncoding);

        executePreparator(preparator);
        countErrors((int) newdata.count());
    }

    @Test
    public void testWrongSourceEncoding() throws Exception {
        String oldEncoding = "ASCII";
        String newEncoding = "UTF-8";
        ChangeEncoding preparator = new ChangeEncoding("bio_path", ChangeEncodingMode.SOURCEANDTARGET, oldEncoding, newEncoding);

        executePreparator(preparator);
        countErrors((int) pipeline.getRawData().count());
    }


    /* Test malformed parameters */

    @Test(expected = ParameterNotSpecifiedException.class)
    public void testNullProperty() throws Exception {
        String newEncoding = "UTF-8";
        ChangeEncoding preparator = new ChangeEncoding(null, ChangeEncodingMode.GIVENTARGET, newEncoding);
        executePreparator(preparator);
    }

    @Test(expected = ParameterNotSpecifiedException.class)
    public void testNullMode() throws Exception {
        String oldEncoding = "cp1252";
        String newEncoding = "UTF-8";
        ChangeEncoding preparator = new ChangeEncoding("bio_path", null, oldEncoding, newEncoding);
        executePreparator(preparator);
    }

    @Test(expected = ParameterNotSpecifiedException.class)
    public void testNullSourceEnc() throws Exception {
        String newEncoding = "UTF-8";
        ChangeEncoding preparator = new ChangeEncoding("bio_path", ChangeEncodingMode.SOURCEANDTARGET, null, newEncoding);
        executePreparator(preparator);
    }

    @Test(expected = ParameterNotSpecifiedException.class)
    public void testNullDestEnc() throws Exception {
        String oldEncoding = "cp1252";
        ChangeEncoding preparator = new ChangeEncoding("bio_path", ChangeEncodingMode.SOURCEANDTARGET, oldEncoding, null);
        executePreparator(preparator);
    }

    @Test(expected = EncodingNotSupportedException.class)
    public void testInvalidSourceEnc() throws Exception {
        String oldEncoding = "not a real encoding";
        String newEncoding = "UTF-8";
        ChangeEncoding preparator = new ChangeEncoding("bio_path", ChangeEncodingMode.SOURCEANDTARGET, oldEncoding, newEncoding);
        executePreparator(preparator);
    }

    @Test(expected = EncodingNotSupportedException.class)
    public void testInvalidDestEnc() throws Exception {
        String newEncoding = "not a real encoding";
        ChangeEncoding preparator = new ChangeEncoding("bio_path", ChangeEncodingMode.GIVENTARGET, newEncoding);
        executePreparator(preparator);
    }

    @Test(expected = Exception.class)  // TODO something should happen
    public void testInvalidProperty() throws Exception {
        String newEncoding = "UTF-8";
        ChangeEncoding preparator = new ChangeEncoding("fake property", ChangeEncodingMode.GIVENTARGET, newEncoding);
        executePreparator(preparator);
    }

    /* Helpers */

    private void testWorkingPreparator(ChangeEncoding preparator, Charset oldCharset, Charset newCharset) throws Exception {
        executePreparator(preparator);
        countErrors(0);

        newPaths = getPaths();
        for (int i = 0; i < oldPaths.length; i++) {
            Assert.assertNotEquals(oldPaths[i], newPaths[i]);
            Assert.assertEquals(readFile(oldPaths[i], oldCharset), readFile(newPaths[i], newCharset));
        }
    }

    // The error repository is too convoluted to recreate, just count the errors and pray to god it's the right ones
    private void countErrors(int errorCount) {
        Assert.assertEquals(errorCount, pipeline.getErrorRepository().getErrorLogs().size());
    }

    private void executePreparator(ChangeEncoding preparator) throws Exception {
        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
    }

    private String readFile(String path, Charset encoding) throws FileNotFoundException {
        FileInputStream is = new FileInputStream(path);
        InputStreamReader isr = new InputStreamReader(is, encoding);
        return new Scanner(isr).useDelimiter("\\A").next();
    }

    private static String[] getPaths() {
        return pipeline.getRawData()
                .select("bio_path")
                .collectAsList()
                .stream()
                .map(row -> row.getString(0))
                .toArray(String[]::new);
    }
}
