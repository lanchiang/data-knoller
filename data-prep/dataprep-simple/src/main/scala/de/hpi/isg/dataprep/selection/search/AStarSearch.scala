package de.hpi.isg.dataprep.selection.search

import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.selection.Preparation

class AStarSearch extends Search[Preparation[_], Schema] with PrepDistance with SchemaHeursitic {
  override def getNext(): (Preparation[_], Schema) = ???
}

trait PrepDistance extends EdgeDistance[Preparation[_]] {
  override def calculateDistance(vertex: Preparation[_]): Double = ???
}

trait SchemaHeursitic extends GraphHeuristic[Schema] {
  override def distance(start: Schema, goal: Schema): Double = ???
}