package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultMapNGramImpl

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class MapNGram(val propertyName : String,
               val n : Int) extends Preparator {

    override def newImpl = new DefaultMapNGramImpl

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