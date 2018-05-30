package de.hpi.isg.dataprep.model.targets

import java.util.Calendar

/**
  * @author Lan Jiang
  * @since 2018/4/27
  */
class Provenance(_name: String, _inputData : DataEntity, _outputData : DataEntity)
  extends Target {

  override var name: String = _name
  var time = Calendar.getInstance().getTime
  var externalResource: String = _
  var inputData = _inputData
  var outputData = _outputData
  override var description: String = _

  override def getName(): String = {
    name
  }

  override def getDescription(): String = {
    description
  }

  override def setDescription(description: String): Unit = {
    this.description = description
  }
}
