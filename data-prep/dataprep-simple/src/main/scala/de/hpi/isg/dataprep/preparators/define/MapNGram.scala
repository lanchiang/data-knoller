package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator

import de.hpi.isg.dataprep.preparators.implementation.DefaultMapNGramImpl

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class MapNGram(val propertyName: String,
               val n: Int) extends AbstractPreparator {

  //    override def newImpl = new DefaultMapNGramImpl

  def this(propertyName: String) {
    this(propertyName, MapNGram.DEFAULT_N)
  }

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {

  }
}

object MapNGram {

  private val DEFAULT_N = 2
}