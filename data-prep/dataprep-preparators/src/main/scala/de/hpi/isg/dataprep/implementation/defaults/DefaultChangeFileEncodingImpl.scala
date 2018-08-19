package de.hpi.isg.dataprep.implementation.defaults

import de.hpi.isg.dataprep.Consequences
import de.hpi.isg.dataprep.implementation.abstracts.ChangeFileEncodingImpl
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import org.apache.spark.sql.{Dataset, Row}

/**
  * @author Lan Jiang
  * @since 2018/8/19
  */
class DefaultChangeFileEncodingImpl extends ChangeFileEncodingImpl {

    override def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        null
    }
}
