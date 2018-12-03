package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.util.DataType.PropertyType

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

class SplitFile (val fileSeparator : String = "") extends Preparator{
  override def buildMetadataSetup(): Unit ={
    val prerequisites = ListBuffer[Metadata]()
    val toChange = ListBuffer[Metadata]()

    prerequisites += new PropertyDataType(fileSeparator, PropertyType.STRING)

    if(fileSeparator == null){
      throw new ParameterNotSpecifiedException("If you dont wont to use the separator please just leave out the field.")
    }

    this.prerequisites.addAll(prerequisites.toList.asJava)
    this.updates.addAll(toChange.toList.asJava)
  }
}