package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import org.reflections.Reflections
import scala.collection.JavaConverters._

trait PreparatorLoader {
  val path: String
  val preparator: Set[Class[_ <: AbstractPreparator]] = loadAllPreparator()

  private def loadAllPreparator(): Set[Class[_ <: AbstractPreparator]] = {
    val reflections = new Reflections(path)

    reflections.getSubTypesOf(classOf[AbstractPreparator]).asScala.toSet
  }

}
