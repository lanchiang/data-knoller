package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.Schema
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultMapNGramImpl
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class MapNGram(val propertyName: String,
               val n: Int) extends AbstractPreparator {

  //    override def newImpl = new DefaultMapNGramImpl

  def this(propertyName: String) {
    this(propertyName, MapNGram.DEFAULT_N)
  }

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
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

object MapNGram {

  private val DEFAULT_N = 2
}