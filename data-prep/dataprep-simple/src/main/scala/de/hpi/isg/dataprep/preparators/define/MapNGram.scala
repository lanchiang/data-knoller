package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.repository.MetadataRepository
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparator}
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

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata], pipeline: AbstractPipeline): Float = {
    0
  }
}

object MapNGram {

  private val DEFAULT_N = 2
}