package de.hpi.isg.dataprep.model.target.system;

import de.hpi.isg.dataprep.ExecutionContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Implementing this interface enables preparator execution.
 *
 * @author Lan Jiang
 * @since 2018/6/4
 */
public interface Executable {

    ExecutionContext execute(Dataset<Row> dataset) throws Exception;
}
