package de.hpi.isg.dataprep.model.targets

import scala.collection.mutable.ListBuffer

/**
  * @author Lan Jiang
  * @since 2018/5/30
  */
abstract class Repository extends Target {

  /**
    * The pool to store all the information of the repository.
    */
  var targets : ListBuffer[Target]

  /**
    * This method adds a piece of {@link Target} into the {@link Repository}.
    * @param target
    * @return
    */
  def addIntoRepository(target: Target) : Boolean
}
