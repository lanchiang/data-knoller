package de.hpi.isg.dataprep.selection.search

trait Search[Node, Edge] {
  def getNext(start: Node): (Edge, Node)
}
