package de.hpi.isg.dataprep.model.targets

import de.hpi.isg.dataprep.util.{Describable, Nameable}

/**
  * A target represents a generic entity.
  *
  * @author Lan Jiang
  * @since 2018/5/30
  */
abstract class Target
  extends Nameable
    with Describable {

  var name : String
  var description : String
}
