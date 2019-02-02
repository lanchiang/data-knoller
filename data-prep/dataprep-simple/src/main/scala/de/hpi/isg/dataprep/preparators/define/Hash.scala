package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultHashImpl
import de.hpi.isg.dataprep.util.HashAlgorithm
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Lan Jiang
  * @since 2018/9/4
  */
class Hash(val propertyName: String,
           val hashAlgorithm: HashAlgorithm) extends AbstractPreparator {

  def this(propertyName: String) {
    this(propertyName, Hash.DEFAULT_ALGORITHM)
  }

  def this() {
    this(null, null)
  }

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    0
  }
}

object Hash {
  val DEFAULT_ALGORITHM = HashAlgorithm.MD5
}