package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}
import java.util.{ArrayList, List}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.preparators.implementation.DefaultPaddingImpl
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DataType.PropertyType
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class Padding(val propertyName: String,
              val expectedLength: Int,
              val padder: String) extends AbstractPreparator {

  def this(propertyName: String, expectedLength: Int) {
    this(propertyName, expectedLength, Padding.DEFAULT_PADDER)
  }

  //    override def newImpl = new DefaultPaddingImpl
  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new util.ArrayList[Metadata]
    val tochange = new util.ArrayList[Metadata]

    if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("%s not specified.", propertyName))
    // illegal padding length was input.
    if (expectedLength <= 0) throw new IllegalArgumentException(String.format("Padding length is illegal!"))

    prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

    // when basic statistics is implemented, one shall be capable of retrieving value length from the metadata repository
    // therefore, this method shall compare the value length as well.

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
  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    0
  }
}

object Padding {

  val DEFAULT_PADDER = "0"
}