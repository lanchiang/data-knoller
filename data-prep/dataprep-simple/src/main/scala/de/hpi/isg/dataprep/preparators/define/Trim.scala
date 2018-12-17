package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}
import java.util.{ArrayList, List}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.preparators.implementation.DefaultTrimImpl
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Lan Jiang
  * @since 2018/9/2
  */
class Trim(val propertyName: String) extends AbstractPreparator {

  //    override def newImpl = new DefaultTrimImpl

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new util.ArrayList[Metadata]
    val tochanges = new util.ArrayList[Metadata]

    if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("%s not specified.", propertyName))
    // Trim can only be applied on String data type. Later version can support trim on other data type
    //        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
    prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

    this.prerequisites.addAll(prerequisites)
    this.updates.addAll(tochanges)
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
