package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}
import java.util.{ArrayList, List}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.preparators.implementation.DefaultRemoveCharactersImpl
import de.hpi.isg.dataprep.util.{DataType, RemoveCharactersMode}
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class RemoveCharacters(val propertyName: String,
                       val mode: RemoveCharactersMode,
                       val userSpecifiedCharacters: String) extends AbstractPreparator {

  def this(propertyName: String, mode: RemoveCharactersMode) {
    this(propertyName, mode, "")
  }

  //    override def newImpl = new DefaultRemoveCharactersImpl

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new util.ArrayList[Metadata]
    val tochange = new util.ArrayList[Metadata]

    if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("ColumnMetadata name not specified."))
    else if (mode == null) throw new ParameterNotSpecifiedException(String.format("Remove character mode not specified."))

    //        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
    prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

    if (mode eq RemoveCharactersMode.CUSTOM) if (userSpecifiedCharacters == null) throw new ParameterNotSpecifiedException(String.format("Characters must be specified if choosing custom mode."))

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
  override def calApplicability(dataset: Dataset[Row], sourceSchema: Schema, targetSchema: Schema, targetMetadata: util.Collection[Metadata]): util.Map[ColumnCombination, lang.Float] = {
    null
  }
}
