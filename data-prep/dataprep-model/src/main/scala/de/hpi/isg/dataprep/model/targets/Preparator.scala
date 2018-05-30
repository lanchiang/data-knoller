package de.hpi.isg.dataprep.model.targets

import de.hpi.isg.dataprep.util.{Describable, Nameable, PreparatorExecutable}

/**
  * @author Lan Jiang
  * @since 2018/5/29
  */
abstract class Preparator
  extends Target
    with Nameable
    with Describable
    with PreparatorExecutable {

  var requiredMetadata : Set[Metadata]
}
