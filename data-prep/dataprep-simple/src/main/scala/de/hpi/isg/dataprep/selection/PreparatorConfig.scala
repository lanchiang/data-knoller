package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.selection.PreparatorType.PreparatorType

abstract class PreparatorConfig(val name: String, val preparatorType: PreparatorType, val columns: Option[Int] = None, val rows: Option[Int]) {
}


object PreparatorType extends Enumeration {
  type PreparatorType = Value
  val TransformRow, TransformColumn, SemanticColumn = Value
}