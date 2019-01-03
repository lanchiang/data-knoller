package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDataType
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.util.DataType.PropertyType
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

class SplitFile(val fileSeparator: String = "") extends AbstractPreparator {
  override def buildMetadataSetup(): Unit = {
    val prerequisites = ListBuffer[Metadata]()
    val toChange = ListBuffer[Metadata]()

    prerequisites += new PropertyDataType(fileSeparator, PropertyType.STRING)

    if (fileSeparator == null) {
      throw new ParameterNotSpecifiedException("If you dont wont to use the separator please just leave out the field.")
    }

    this.prerequisites.addAll(prerequisites.toList.asJava)
    this.updates.addAll(toChange.toList.asJava)
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    0
  }
}