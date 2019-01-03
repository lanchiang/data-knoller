package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.preparators.implementation.DefaultCollapseImpl
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Lan Jiang
  * @since 2018/8/31
  */
class Collapse(val propertyName: String) extends AbstractPreparator {

  //    override def newImpl = new DefaultCollapseImpl

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new util.ArrayList[Metadata]
    val tochanges = new util.ArrayList[Metadata]

    if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("%s not specified", propertyName))
    // Collapse can only be applied on String data type
    //        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))
    prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

    this.prerequisites.addAll(prerequisites)
    this.updates.addAll(tochanges)
  }

    override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
      0
    }
}
