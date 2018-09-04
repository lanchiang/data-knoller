package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.implementation.DefaultHashImpl
import de.hpi.isg.dataprep.model.target.preparator.Preparator
import de.hpi.isg.dataprep.util.HashAlgorithm

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class Hash(val propertyName : String,
           val hashAlgorithm: HashAlgorithm) extends Preparator {

    this.impl = new DefaultHashImpl

    def this(propertyName: String) {
        this(propertyName, Hash.DEFAULT_ALGORITHM)
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

object Hash {
    val DEFAULT_ALGORITHM = HashAlgorithm.MD5
}