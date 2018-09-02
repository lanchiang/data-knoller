package de.hpi.isg.dataprep.implementation

import de.hpi.isg.dataprep.Consequences
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.preparator.{Preparator, PreparatorImpl}
import de.hpi.isg.dataprep.preparators.RemoveCharacters
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.util.CollectionAccumulator

/**
  *
  * @author Lan Jiang
  * @since 2018/8/30
  */
class DefaultRemoveCharactersImpl extends PreparatorImpl {


    override protected def executePreparator(preparator: Preparator, dataFrame: Dataset[Row]): Consequences = {
        val preparator_ = this.getPreparatorInstance(preparator, classOf[RemoveCharacters])
        val errorAccumulator = this.createErrorAccumulator(dataFrame)
        executeLogic(preparator_, dataFrame, errorAccumulator)
    }

    private def executeLogic(preparator: RemoveCharacters, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): Consequences = {
        val propertyName = preparator.propertyName
        val removeCharactersMode = preparator.mode
        val userSpecifiedCharacters = preparator.userSpecifiedCharacters

        val createdRDD = dataFrame.rdd.flatMap(row => {
            

            None
        })

        new Consequences(null, null)
    }
}
