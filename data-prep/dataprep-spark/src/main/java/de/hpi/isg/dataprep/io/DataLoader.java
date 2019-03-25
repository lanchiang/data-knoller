package de.hpi.isg.dataprep.io;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.io.context.DataContext;
import de.hpi.isg.dataprep.io.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.io.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import org.apache.log4j.Logger;

/**
 * @author Lan Jiang
 * @since 2019-03-22
 */
public class DataLoader {

    protected static String resourcePath;

    protected static FileLoadDialect dialect;

    protected static DataContext dataContext;

    private final Logger logger = Logger.getLogger(DataLoader.class);

    public void createDataContext() {
        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect);
        dataContext = dataLoader.load();
    }

    /**
     * Load the settings of parameters for the spark reader from a file
     *
     * @param dialectFilePath is the parameter value file
     */
    public void loadDialectFromFile(String dialectFilePath) {
        if (dialect != null) {
            logger.warn("Dialect has already been set!");
            return;
        }

        dialect = new DialectBuilder().url(resourcePath).buildDialect();
    }
}
