package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.implementation.DefaultMovePropertyImpl

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class MoveProperty(val propertyName: String,
                   val newPosition: Int) extends AbstractPreparator {

  def this(propertyName: String) {
    this(propertyName, MoveProperty.DEFAULT_NEW_POSITION)
  }

  //    override def newImpl = new DefaultMovePropertyImpl

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {

  }
}

object MoveProperty {
  val DEFAULT_NEW_POSITION = 0
}

