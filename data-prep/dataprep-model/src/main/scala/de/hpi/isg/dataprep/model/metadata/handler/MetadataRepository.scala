package de.hpi.isg.dataprep.model.metadata.handler

import de.hpi.isg.dataprep.exceptions.TargetTypeUnmatchException
import de.hpi.isg.dataprep.model.targets.{Metadata, Repository, Target}

import scala.collection.mutable.ListBuffer

/**
  * @author Lan Jiang
  * @since 2018/5/29
  */
class MetadataRepository extends Repository {

  /**
    * This method adds a piece of {@link Target} into the {@link Repository}.
    *
    * @param target represents the piece of information to be added into the repository.
    * @return whether this operation succeeds.
    */
  override def addIntoRepository(target: Target): Boolean = {
    var success = false
    if (!target.isInstanceOf[Metadata]) {
      throw new TargetTypeUnmatchException("The added target does not match the repository type!")
    } else {
      targets += target.asInstanceOf[Metadata]
      success = true
    }
    success
  }

  /**
    * The pool to store all the information of the repository.
    */
  override var targets: ListBuffer[Target] = ListBuffer()
  override var name: String = _
  override var description: String = _

  override def getDescription(): String = ???

  override def setDescription(description: String): Unit = ???

  override def getName(): String = ???
}
