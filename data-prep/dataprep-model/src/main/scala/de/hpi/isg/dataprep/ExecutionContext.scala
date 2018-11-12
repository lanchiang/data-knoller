package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.errorlog.ErrorLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.CollectionAccumulator

/**
  * @author Lan Jiang
  * @since 2018/8/7
  */
class ExecutionContext(val newDataFrame: DataFrame,
                       val errorsAccumulator: CollectionAccumulator[_ <: PreparationError]) {

    def hasError(): Boolean = {
        return !errorsAccumulator.isZero
    }
}
