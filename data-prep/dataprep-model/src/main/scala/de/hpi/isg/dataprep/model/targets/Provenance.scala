package de.hpi.isg.dataprep.model.targets

import de.hpi.isg.dataprep.util.{Describable, Nameable}

/**
  * @author Lan Jiang
  * @since 2018/4/27
  */
abstract class Provenance
  extends Target
    with Nameable
    with Describable {

  var preparator: String
  var time: String
  var externalResourse: String
  var inputData: String
  var outputData: String
  var description: String
}
