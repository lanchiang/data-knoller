package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.util.DataType.PropertyType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

class ChangeDateFormat(val propertyName: String,
                       val sourceDatePattern: Option[DatePatternEnum] = None,
                       val targetDatePattern: Option[DatePatternEnum] = None) extends AbstractPreparator {
  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of
    * metadata into both prerequisite and toChange set.
    *
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

  /**
    * Calculate the matrix of preparator applicability to the data. In the matrix, each
    * row represent a specific signature of the preparator, while each column represent a specific
    * {@link ColumnCombination} of the data
    *
    * @return the applicability matrix succinctly represented by a hash map. Each key stands for
    *         a { @link ColumnCombination} in the dataset, and its value the applicability score of this preparator signature.
    */
  override def calApplicability(): util.Map[ColumnCombination, lang.Float] = {
    null
  }
}

object ChangeDateFormat {
  def apply(
             propertyName: String,
             sourceDatePattern: DatePatternEnum,
             targetDatePattern: DatePatternEnum
           ): ChangeDateFormat = {
    new ChangeDateFormat(propertyName, Option(sourceDatePattern), Option(targetDatePattern))
  }

  def apply(propertyName: String, targetDatePattern: DatePatternEnum): ChangeDateFormat = {
    new ChangeDateFormat(propertyName, targetDatePattern = Option(targetDatePattern))
  }
}
