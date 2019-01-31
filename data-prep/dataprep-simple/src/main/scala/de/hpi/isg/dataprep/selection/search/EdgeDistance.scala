package de.hpi.isg.dataprep.selection.search

trait EdgeDistance[Vertex] {
  def calculateDistance(vertex: Vertex): Double
}
