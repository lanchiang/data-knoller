package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.selection.PreparationType.PreparationType

abstract class PreparationConfig(val name: String, val preparationType:PreparationType, val columns: Option[Int] = None, val rows: Option[Int]) {
}


object PreparationType extends Enumeration {
  type PreparationType = Value
  val TransformRow, TransformColumn, SemanticColumn = Value
}