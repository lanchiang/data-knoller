package de.hpi.isg.dataprep.selection.search

trait NodeDistance[Node, Edge] {
  def calculateDistance(start: Node, goal: Node, edge: Edge): Double
}
