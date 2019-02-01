package de.hpi.isg.dataprep.selection.search

import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.selection.Preparator

class AStarSearch extends Search[Schema, Preparator]  {
  override def getNext(schema: Schema): (Schema, Preparator) = {

  }
}
