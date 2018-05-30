package de.hpi.isg.dataprep.model.targets

import de.hpi.isg.dataprep.util.Valued

/**
  * @author Lan Jiang
  * @since 2018/5/29
  */
abstract class Metadata
  extends Target
    with Valued {

  var value : String

  override def toString: String = this.getName()
}
