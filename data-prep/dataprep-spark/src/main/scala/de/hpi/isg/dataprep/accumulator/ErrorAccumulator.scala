package de.hpi.isg.dataprep.accumulator

import de.hpi.isg.dataprep.model.error.PreparationError
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.CollectionAccumulator

/**
  * An [[ErrorAccumulator]] records all the preparation errors captured while exeucting a preparator.
  * In the preparator logic implementation, an [[ErrorAccumulator]] is created and passed as a parameter
  * of an [[de.hpi.isg.dataprep.ExecutionContext]].
  *
  * @author Lan Jiang
  * @since 2018/9/17
  */
class ErrorAccumulator(val dataFrame: DataFrame, val errorAccumulatorName: String) {

  private val errorAccumulator = createErrorAccumulator()

  def this(dataFrame: DataFrame) {
    this(dataFrame, ErrorAccumulator.DEFAULT_ACCUMULATOR_NAME)
  }

  private def createErrorAccumulator(): CollectionAccumulator[PreparationError] = {
    val errorAccumulator = new CollectionAccumulator[PreparationError]
    dataFrame.sparkSession.sparkContext.register(errorAccumulator, errorAccumulatorName)
    errorAccumulator
  }

  def addErrorLog(error: PreparationError): Unit = {
    this.errorAccumulator.add(error)
  }
}

object ErrorAccumulator {
  private val DEFAULT_ACCUMULATOR_NAME = "Default error accumulator"
}