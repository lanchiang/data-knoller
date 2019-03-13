package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import org.reflections.Reflections
import scala.collection.JavaConverters._

/**
  * The trait can be used to load preparator via reflection for a given path
  */

trait PreparatorLoader {
  val path: String
  val preparators: List[Class[_ <: AbstractPreparator]] = loadAllPreparator()

  /**
    * loads preparator via reflection
    *
    * @return list of all classes that inherit AbstractPreparator in given path
    */
  private def loadAllPreparator(): List[Class[_ <: AbstractPreparator]] = {
    val reflections = new Reflections(path)
    reflections.getSubTypesOf(classOf[AbstractPreparator]).asScala.toList
  }

}
