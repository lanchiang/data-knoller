package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.CollectionAccumulator

/**
  * An execution context maintains the information that are changed after executing a preparator. In the implementation
  * of each preparator, an [[ExecutionContext]] instance is created to record the changes in metadata, data itself, and
  * the errors produced by executing the preparator. The [[ExecutionContext]] is returned by the preparator to the pipeline,
  * by which update the metadata repository, error repository, and data later.
  *
  * @author Lan Jiang
  * @since 2018/8/7
  */
class ExecutionContext(val newDataFrame: DataFrame,
                       val errorsAccumulator: CollectionAccumulator[_ <: PreparationError]) {

  // Todo: metadata is still not included as a member variable of this class.

  def hasError(): Boolean = {
    return !errorsAccumulator.isZero
  }
}
