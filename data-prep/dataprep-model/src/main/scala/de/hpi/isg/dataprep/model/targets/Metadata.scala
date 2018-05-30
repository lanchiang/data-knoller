package de.hpi.isg.dataprep.model.targets

import de.hpi.isg.dataprep.util.Nameable

/**
  * @author Lan Jiang
  * @since 2018/5/29
  */
abstract class Metadata extends Target with Nameable {

  var name : String

  override def toString: String = name
}
