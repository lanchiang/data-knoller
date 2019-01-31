package de.hpi.isg.dataprep.selection.search

trait GraphHeuristic[Edge] {
  def distance(start: Edge, goal: Edge): Double
}
