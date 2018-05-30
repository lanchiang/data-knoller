package de.hpi.isg.dataprep.model.provenance

import de.hpi.isg.dataprep.exceptions.TargetTypeUnmatchException
import de.hpi.isg.dataprep.model.targets.{Metadata, Provenance, Repository, Target}

import scala.collection.mutable.ListBuffer

/**
  * @author Lan Jiang
  * @since 2018/4/27
  */
class ProvenanceRepository extends Repository {

  /**
    * The pool to store all the information of the repository.
    */
  override var targets: ListBuffer[Target] = ListBuffer()
  override var name: String = _
  override var description: String = _

  /**
    * This method adds a piece of {@link Target} into the {@link Repository}.
    *
    * @param target represents the piece of information to be added into the repository.
    * @return whether this operation succeeds.
    */
  override def addIntoRepository(target: Target): Boolean = {
    var success = false
    if (!target.isInstanceOf[Provenance]) {
      throw new TargetTypeUnmatchException("The added target does not match the repository type!")
    } else {
      targets += target.asInstanceOf[Provenance]
      success = true
    }
    success
  }

  override def getDescription(): String = {
    this.description
  }

  override def setDescription(description: String): Unit = {
    this.description = description
  }

  override def getName(): String = {
    this.name
  }
}
