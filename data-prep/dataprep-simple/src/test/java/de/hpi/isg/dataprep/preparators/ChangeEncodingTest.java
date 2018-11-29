package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.components.Preparator;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Char;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Lan Jiang
 * @since 2018/9/4
 */
public class ChangeEncodingTest extends PreparatorTest {
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
    }

    @Test
    public void testChangeKnownEncoding() throws Exception {
        pipeline.executePipeline();
        oldPaths = getPaths();

        Charset oldEncoding = Charset.forName("UTF-16");
        Charset newEncoding = Charset.forName("UTF-8");
        Preparator preparator = new ChangeEncoding("bio", oldEncoding, newEncoding);

        AbstractPreparation preparation = new Preparation(preparator);
        pipeline.addPreparation(preparation);
        pipeline.executePipeline();

        pipeline.getRawData().show();

        List<ErrorLog> errorLogs = new ArrayList<>();
        ErrorRepository errorRepository = new ErrorRepository(errorLogs);

        Assert.assertEquals(errorRepository, pipeline.getErrorRepository());

        String[] newPaths = getPaths();
        for (int i = 0; i < oldPaths.length; i++) {
            Assert.assertEquals(readFile(oldPaths[i], oldEncoding), readFile(newPaths[i], newEncoding));
        }
    }

    private String readFile(String path, Charset encoding) throws FileNotFoundException {
        FileInputStream is = new FileInputStream(path);
        InputStreamReader isr = new InputStreamReader(is, encoding);
        String fileContent = new Scanner(isr).useDelimiter("\\A").next();
        return fileContent;
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
