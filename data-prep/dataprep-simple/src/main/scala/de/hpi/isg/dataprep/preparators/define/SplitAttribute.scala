package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitAttributeImpl
import org.apache.spark.sql.{Dataset, Row}

class SplitAttribute(val propertyName: String,
                     val separator: String,
                     val startLeft: Boolean,
                     val times: Int
                    ) extends AbstractPreparator {

  def this(propertyName: String, separator: String) {
    this(propertyName, separator, true, -1)
  }

  def this(propertyName: String) {
    this(propertyName, null, true, -1)
  }

  this.impl = new DefaultSplitAttributeImpl

  /**
    * This method validates the input parameters of a {@link AbstractPreparator}. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws ParameterNotSpecifiedException
    */
  override def buildMetadataSetup(): Unit = {

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

object SplitAttribute {}