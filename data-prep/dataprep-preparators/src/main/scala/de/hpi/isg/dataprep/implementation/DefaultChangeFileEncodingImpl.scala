package de.hpi.isg.dataprep.implementation

import de.hpi.isg.dataprep.Consequences
import de.hpi.isg.dataprep.SparkPreparators.spark
import de.hpi.isg.dataprep.model.target.preparator.{Preparator, PreparatorImpl}
import de.hpi.isg.dataprep.preparators.ChangeFileEncoding
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.Try

/**
  * @author Lan Jiang
  * @since 2018/8/19
  */
class DefaultChangeFileEncodingImpl extends PreparatorImpl {

    override def executePreparator(preparator: Preparator, dataFrame: DataFrame): Consequences = {
        val errorAccumulator = new CollectionAccumulator[(Any, Throwable)]
        dataFrame.sparkSession.sparkContext.register(errorAccumulator,
            "The error accumulator for preparator: Change file encoding.")

        val preparator_ = preparator.asInstanceOf[ChangeFileEncoding]
        val targetFileEncoding_ = preparator_.targetEncoding

        val rowEncoder = RowEncoder(dataFrame.schema)

        val createdRDD = dataFrame.filter(row => {
            val tryConvert = Try{

            }
            true
        })

//        new Consequences(createdRDD, errorAccumulator)
        new Consequences(dataFrame, null)
    }
}
