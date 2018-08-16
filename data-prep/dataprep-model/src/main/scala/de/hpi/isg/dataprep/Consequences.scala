package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.model.target.error.ErrorLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.CollectionAccumulator

/**
  * @author Lan Jiang
  * @since 2018/8/7
  */
class Consequences(newDataFrame: DataFrame, errorsAccumulator: CollectionAccumulator[(Any, Throwable)]) {

    def this(errorsAccumulator: CollectionAccumulator[(Any, Throwable)]) {
        this(_, errorsAccumulator)
    }

    var errorsAccumulator_ = errorsAccumulator
    var newDataFrame_ = newDataFrame

    def hasError(): Boolean = {
        return !errorsAccumulator_.isZero
    }
}
