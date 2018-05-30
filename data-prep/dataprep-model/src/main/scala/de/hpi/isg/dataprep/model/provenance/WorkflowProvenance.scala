package de.hpi.isg.dataprep.model.provenance

import de.hpi.isg.dataprep.model.targets.Provenance

/**
  * @author Lan Jiang
  * @since 2018/4/27
  */
class WorkflowProvenance extends Provenance {
  override var preparator: String = _
  override var time: String = _
  override var externalResourse: String = _
  override var inputData: String = _
  override var outputData: String = _
  override var description: String = _

  override def getDescription(): String = {
    this.description
  }

  override def setDescription(description: String): Unit = {
    this.description = description
  }

  override def getName(): String = {
    ""
  }
}
