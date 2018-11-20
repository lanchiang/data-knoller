package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator

class SplitFile (val fileSeparator : String = "") extends Preparator{
  override def buildMetadataSetup(): Unit ={
    // TODO: implement metadata stuff
  }
}