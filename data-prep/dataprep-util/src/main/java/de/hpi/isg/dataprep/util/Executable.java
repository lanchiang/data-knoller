package de.hpi.isg.dataprep.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Implementing this interface enables preparator execution.
 *
 * @author Lan Jiang
 * @since 2018/6/4
 */
public interface Executable {

    void execute() throws Exception;
}
