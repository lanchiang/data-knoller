package de.hpi.isg.dataprep.model.targets

/**
  * @author Lan Jiang
  * @since 2018/5/30
  */
abstract class Repository {

  /**
    * This method adds a piece of {@link Target} into the {@link Repository}.
    * @param target
    * @return
    */
  def addIntoRepository(target: Target) : Boolean
}
