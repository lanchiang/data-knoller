package de.hpi.isg.dataprep.model.targets

import de.hpi.isg.dataprep.model.error.ErrorType

/**
  * This class represents the error log. Each instance refers to one piece of erroneous content.
 *
  * @author Lan Jiang
  * @since 2018/5/28
  */
abstract class ErrorLog extends Target {

  var starting: String
  var ending: String

  var errorType = new ErrorType
}
