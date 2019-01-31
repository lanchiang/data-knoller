package de.hpi.isg.dataprep.selection.search

trait Search[Vertex, Edge] {
  def getNext(): (Vertex, Edge)
}
