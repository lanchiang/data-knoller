package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}
import java.util.{ArrayList, List}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.preparators.implementation.DefaultReplaceSubstringImpl
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Lan Jiang
  * @since 2018/9/2
  */
class ReplaceSubstring(val propertyName: String,
                       val source: String,
                       val replacement: String,
                       val times: Int) extends AbstractPreparator {

  //    override def newImpl = new DefaultReplaceSubstringImpl

  def this(propertyName: String, source: String, replacement: String) {
    this(propertyName, source, replacement, ReplaceSubstring.DEFAULT_TIMES)
  }

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new util.ArrayList[Metadata]
    val tochange = new util.ArrayList[Metadata]

    if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("ColumnMetadata name not specified"))
    if (source == null) throw new ParameterNotSpecifiedException(String.format("Source sub-string not specified"))
    if (replacement == null) throw new ParameterNotSpecifiedException(String.format("Target sub-string not specified"))
    if (times < 0) throw new IllegalArgumentException(String.format("Cannot replace the first minus sub-strings."))

    //        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
    prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

    this.prerequisites.addAll(prerequisites)
    this.updates.addAll(tochange)
  }

  /**
    * Calculate the matrix of preparator applicability to the data. In the matrix, each
    * row represent a specific signature of the preparator, while each column represent a specific
    * {@link ColumnCombination} of the data
    *
    * @return the applicability matrix succinctly represented by a hash map. Each key stands for
    *         a { @link ColumnCombination} in the dataset, and its value the applicability score of this preparator signature.
    */
  override def calApplicability(dataset: Dataset[Row], sourceSchema: Schema, targetSchema: Schema, targetMetadata: util.Collection[Metadata]): Float = {
    null
  }
}

object ReplaceSubstring {

  private val DEFAULT_TIMES = 0 // if default_times is used. Replace all the found source string with replacement.
}
