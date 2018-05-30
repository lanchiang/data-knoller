package de.hpi.isg.dataprep.model.targets

import de.hpi.isg.dataprep.util.PreparatorExecutable

/**
  * @author Lan Jiang
  * @since 2018/5/29
  */
abstract class Preparator
  extends Target
    with PreparatorExecutable {

  var requiredMetadata : Set[Metadata]
  var actionScope : DataEntity
  var changedMetadata : Set[Metadata]
}

object Preparator {

}