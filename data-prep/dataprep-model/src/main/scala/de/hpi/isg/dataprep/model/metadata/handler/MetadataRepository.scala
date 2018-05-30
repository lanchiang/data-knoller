package de.hpi.isg.dataprep.model.metadata.handler

import de.hpi.isg.dataprep.exceptions.TargetTypeUnmatchException
import de.hpi.isg.dataprep.model.targets.{Metadata, Repository, Target}

/**
  * @author Lan Jiang
  * @since 2018/5/29
  */
class MetadataRepository extends Repository {

  private var metadata : Set[Metadata] = Set()

  /**
    * This method adds a piece of {@link Target} into the {@link Repository}.
    *
    * @param target
    * @return
    */
  override def addIntoRepository(target: Target): Boolean = {
    var success = true
    if (!target.isInstanceOf[Metadata]) {
      throw new TargetTypeUnmatchException("The added target does not match the repository type!")
    }
    
    success
  }
}
