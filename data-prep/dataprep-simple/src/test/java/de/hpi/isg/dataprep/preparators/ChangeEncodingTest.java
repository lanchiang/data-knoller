package de.hpi.isg.dataprep.preparators;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Preparation;
import de.hpi.isg.dataprep.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.repository.ErrorRepository;
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog;
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation;
import de.hpi.isg.dataprep.preparators.define.ChangeEncoding;
import de.hpi.isg.dataprep.util.ChangeEncodingMode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

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
		String oldEncoding = "UTF-16";
		String newEncoding = "UTF-8";
		ChangeEncoding preparator = new ChangeEncoding("bio_path", ChangeEncodingMode.SOURCEANDTARGET, oldEncoding, newEncoding);
		testPreparator(preparator, Charset.forName(oldEncoding), Charset.forName(newEncoding));
	}

	@Test
	public void testChangeUnknownEncoding() throws Exception {
        String oldEncoding = "UTF-16";
        String newEncoding = "UTF-8";
		ChangeEncoding preparator = new ChangeEncoding("bio_path", ChangeEncodingMode.GIVENTARGET, newEncoding);
		testPreparator(preparator, Charset.forName(oldEncoding), Charset.forName(newEncoding));
	}


	private void testPreparator(ChangeEncoding preparator, Charset oldCharset, Charset newCharset) throws Exception {
        pipeline.executePipeline();
        oldPaths = getPaths();

		AbstractPreparation preparation = new Preparation(preparator);
		pipeline.addPreparation(preparation);
		pipeline.executePipeline();

		pipeline.getRawData().show();

		List<ErrorLog> errorLogs = new ArrayList<>();
		ErrorRepository errorRepository = new ErrorRepository(errorLogs);

		Assert.assertEquals(errorRepository, pipeline.getErrorRepository());

		String[] newPaths = getPaths();
		for (int i = 0; i < oldPaths.length; i++) {
			Assert.assertEquals(readFile(oldPaths[i], oldCharset), readFile(newPaths[i], newCharset));
		}
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
