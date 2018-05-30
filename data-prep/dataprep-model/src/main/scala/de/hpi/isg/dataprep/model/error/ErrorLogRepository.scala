package de.hpi.isg.dataprep.model.error

import de.hpi.isg.dataprep.model.error.DataEntity.DataEntity
import de.hpi.isg.dataprep.model.targets.{ErrorLog, Repository}

import scala.collection.mutable.ListBuffer

/** The instance of this class represents the repository of the error logs. Each repository
  * includes the error logs of one data-set.
  *
  * @author Lan Jiang
  * @since 2018/5/28
  */
class ErrorLogRepository(de: DataEntity) extends Repository {

  private val dataEntity = de

  private var errorLogRepository: ListBuffer[ErrorLog] = ListBuffer()

  def addNewError(errorLog: ErrorLog): Unit = {
    errorLogRepository += errorLog
  }
}
