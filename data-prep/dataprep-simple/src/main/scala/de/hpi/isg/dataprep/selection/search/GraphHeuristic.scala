package de.hpi.isg.dataprep.selection.search

trait GraphHeuristic[Node] {
  def distance(start: Node, goal: Node): Double
}
