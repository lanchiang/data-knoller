package de.hpi.isg.dataprep.implementation

import de.hpi.isg.dataprep.Consequences
import de.hpi.isg.dataprep.SparkPreparators.spark
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.preparators.ChangeFileEncoding
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.util.Try

/**
  * @author Lan Jiang
  * @since 2018/8/19
  */
class efaultChangeFileEncodingImpl extends ChangeFileEncodingImpl {

    override def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        val errorAccumulator = new CollectionAccumulator[(Any, Throwable)]
        dataFrame.sparkSession.sparkContext.register(errorAccumulator,
            "The error accumulator for preparator: Change file encoding.")

        val preparator_ = preparator.asInstanceOf[ChangeFileEncoding]
        val targetFileEncoding_ = preparator_.getTargetEncoding

        val createdRDD = dataFrame.rdd.filter(row => {
            val tryConvert = Try{

            }
            true
        })

        val resultDataFrame = spark.createDataFrame(createdRDD, dataFrame.schema)

//        new Consequences(resultDataFrame, errorAccumulator)
        new Consequences(resultDataFrame, null)
    }
}
