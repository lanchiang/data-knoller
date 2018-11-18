package de.hpi.isg.dataprep.preparators.define

import de.hpi.isg.dataprep.components.Preparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

class ChangeDateFormat(val propertyName : String,
                       val sourceDatePattern : Option[DatePatternEnum] = None,
                       val targetDatePattern : Option[DatePatternEnum] = None) extends Preparator {
    /**
      * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of
      * metadata into both prerequisite and toChange set.
      * @throws ParameterNotSpecifiedException
      */
    override def buildMetadataSetup(): Unit = {
        val prerequisites = ListBuffer[Metadata]()
        val toChange = ListBuffer[Metadata]()
        prerequisites += new PropertyDataType(propertyName, PropertyType.STRING)

        if (propertyName == null) {
            throw new ParameterNotSpecifiedException("Property name not specified!")
        }

        sourceDatePattern match {
            case Some(pattern) => prerequisites += new PropertyDatePattern(pattern, new ColumnMetadata(propertyName))
            case None =>
        }

        targetDatePattern match {
            case Some(pattern) => toChange += new PropertyDatePattern(pattern, new ColumnMetadata(propertyName))
            case None => throw new ParameterNotSpecifiedException("Target pattern not specified!")
        }

        this.prerequisites.addAll(prerequisites.toList.asJava)
        this.updates.addAll(toChange.toList.asJava)
    }
}
