package de.hpi.isg.dataprep.util

/**
  * This trait provides classes to be describable.
  * @author Lan Jiang
  * @since 2018/5/29
  */
trait Describable {

  def getDescription(): String
  def setDescription(description: String): Unit

}
