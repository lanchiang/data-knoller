package de.hpi.isg.dataprep.implementation;

import de.hpi.isg.dataprep.Consequences;
import de.hpi.isg.dataprep.model.error.PreparationError;
import de.hpi.isg.dataprep.model.target.preparator.Preparator;
import de.hpi.isg.dataprep.model.target.preparator.PreparatorImpl;
import de.hpi.isg.dataprep.preparators.ChangeFileEncoding;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;

/**
 * The delegation class of {@link ChangeFileEncoding}
 *
 * @author Lan Jiang
 * @since 2018/8/16
 */
abstract public class ChangeFileEncodingImpl extends PreparatorImpl {

    @Override
    protected Consequences executePreparator(Preparator preparator, Dataset<Row> dataFrame) throws Exception {
        return null;
    }

    @Override
    protected CollectionAccumulator<PreparationError> createErrorAccumulator(Preparator preparator, Dataset<Row> dataFrame) {
        return null;
    }
}
