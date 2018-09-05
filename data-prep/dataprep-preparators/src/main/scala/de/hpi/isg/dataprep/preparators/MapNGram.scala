package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.implementation.DefaultMapNGramImpl
import de.hpi.isg.dataprep.model.target.preparator.Preparator

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class MapNGram(val propertyName : String,
               val n : Int) extends Preparator {

    this.impl = new DefaultMapNGramImpl

    def this(propertyName : String) {
        this(propertyName, MapNGram.DEFAULT_N)
    }

    /**
      * This method validates the input parameters of a [[Preparator]]. If succeeds, setup the values of metadata into both
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