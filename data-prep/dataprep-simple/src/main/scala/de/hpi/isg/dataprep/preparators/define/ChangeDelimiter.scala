package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultChangeDelimiterImpl

/**
  * @author Lan Jiang
  * @since 2018/9/17
  */
class ChangeDelimiter(val tableName : String,
                      val sourceDelimiter: String,
                      val targetDelimiter: String) extends Preparator {

    this.impl = new DefaultChangeDelimiterImpl

    def this(tableName : String, targetDelimiter: String) {
        this(tableName, null, targetDelimiter)
    }

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {
    }
}
