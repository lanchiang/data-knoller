package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.model.target.error.ErrorLog
import org.apache.spark.util.CollectionAccumulator

/**
  * @author Lan Jiang
  * @since 2018/8/7
  */
class Consequences(_errorsAccumulator: CollectionAccumulator[(Any, Throwable)]) {

    var errorsAccumulator = _errorsAccumulator

    def hasError(): Boolean = {
        return !errorsAccumulator.isZero
    }
}
