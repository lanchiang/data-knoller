package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ChangeTableEncodingTest extends PreparatorTest {
    private static final String TEST_DATA_PATH = "./src/test/resources/TODO.csv";

    private static final String ENCODING = "TODO";

    @BeforeClass
    public static void setUp() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        FileLoadDialect dialect = new DialectBuilder()
                .hasHeader(true)
                .inferSchema(true)
                .url(TEST_DATA_PATH)
                .encoding(ENCODING)
                .buildDialect();
        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
        dataContext = dataLoader.load();
    }

    @Test
    public void testChangeKnownEncoding() throws Exception {
//        ChangeTableEncoding preparator = new ChangeTableEncoding();
//        AbstractPreparation preparation = new Preparation(preparator);
//        pipeline.addPreparation(preparation);
        pipeline.executePipeline();
        char encodingErrorChar = '�';
        pipeline.getRawData().show();
        List<Boolean> errors = pipeline
                .getRawData()
                .collectAsList()
                .stream()
                .map(row -> row.getString(1).indexOf(encodingErrorChar) != -1)
                .collect(Collectors.toList());
        System.out.println(errors);
    }
}
