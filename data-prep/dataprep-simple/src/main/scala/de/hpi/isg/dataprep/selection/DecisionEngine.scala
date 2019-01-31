package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.system.Engine

abstract class DecisionEngine extends Engine {
  def selectNextPreparation(): Preparation[_]
}

