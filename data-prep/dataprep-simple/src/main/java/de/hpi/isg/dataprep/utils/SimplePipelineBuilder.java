package de.hpi.isg.dataprep.utils;

import de.hpi.isg.dataprep.DialectBuilder;
import de.hpi.isg.dataprep.components.Pipeline;
import de.hpi.isg.dataprep.io.context.DataContext;
import de.hpi.isg.dataprep.io.load.FlatFileDataLoader;
import de.hpi.isg.dataprep.io.load.SparkDataLoader;
import de.hpi.isg.dataprep.model.dialects.FileLoadDialect;
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A utility builder class to build pipeline instance for unit test.
 *
 * @author Lan Jiang
 * @since 2019-04-18
 */
public class SimplePipelineBuilder {

    protected static DataContext dataContext;
    protected static FileLoadDialect dialect;

    public static AbstractPipeline fromParameterBuilder(String resourcePath) {
        return fromParameterBuilder(resourcePath, true);
    }

    public static AbstractPipeline fromParameterBuilder(String resourcePath, boolean hasHeader) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        dialect = new DialectBuilder()
                .hasHeader(hasHeader)
                .inferSchema(true)
                .url(resourcePath)
                .buildDialect();

        SparkDataLoader dataLoader = new FlatFileDataLoader(dialect, new HashSet<>(), new ArrayList<>());
        dataContext = dataLoader.load();
        return new Pipeline(dataContext);
    }
}
