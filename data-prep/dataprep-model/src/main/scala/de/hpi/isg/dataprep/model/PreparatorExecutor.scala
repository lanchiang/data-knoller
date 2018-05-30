package de.hpi.isg.dataprep.model

import de.hpi.isg.dataprep.model.targets.Preparator

/**
  * @author Lan Jiang
  * @since 2018/5/29
  */
object PreparatorExecutor {

  def execute(preparator: Preparator): Unit = {
    preparator.executePreparator()
  }
}
