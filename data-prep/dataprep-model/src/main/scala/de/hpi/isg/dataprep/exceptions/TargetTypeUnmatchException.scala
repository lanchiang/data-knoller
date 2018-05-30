package de.hpi.isg.dataprep.exceptions

/**
  * @author Lan Jiang
  * @since 2018/5/30
  */
class TargetTypeUnmatchException(message: String) extends Exception(message) {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  def this() {
    this(null: String)
  }
}
