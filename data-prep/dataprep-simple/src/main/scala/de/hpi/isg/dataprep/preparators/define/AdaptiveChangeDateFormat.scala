package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultAdaptiveChangeDateFormatImpl
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import de.hpi.isg.dataprep.util.DataType
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Hendrik Rätz, Nils Strelow
  * @since 2018/12/03
  */
class AdaptiveChangeDateFormat(val propertyName : String,
                               val sourceDatePattern : Option[DatePatternEnum] = None,
                               val targetDatePattern: DatePatternEnum) extends AbstractPreparator {

    this.impl = new DefaultAdaptiveChangeDateFormatImpl

    /**
      * This method validates the input parameters of a [[AbstractPreparator]]. If it succeeds, setup the values of metadata into both
      * prerequisite and toChange set.
      *
      * @throws Exception
      */
    override def buildMetadataSetup(): Unit = {
        val prerequisites = new util.ArrayList[Metadata]
        val toChange = new util.ArrayList[Metadata]

        if (propertyName == null) throw new ParameterNotSpecifiedException(String.format("Propertry name not specified."))

        prerequisites.add(new PropertyDataType(propertyName, DataType.PropertyType.STRING))

        sourceDatePattern match {
            case Some(pattern) => prerequisites.add(new PropertyDatePattern(pattern, new ColumnMetadata(propertyName)))
            case None =>
        }

        toChange.add(new PropertyDatePattern(targetDatePattern, new ColumnMetadata(propertyName)))

        this.prerequisites.addAll(prerequisites)
        this.updates.addAll(toChange)
    }

    /**
      * Calculate the applicability score of the preparator on the dataset.
      *
      * @param schemaMapping  is the schema of the input data
      * @param dataset        is the input dataset
      * @param targetMetadata is the set of { @link Metadata} that shall be fulfilled for the output data
      * @return the applicability matrix succinctly represented by a hash map. Each key stands for
      *         a { @link ColumnCombination} in the dataset, and its value the applicability score of this preparator signature.
      */
    override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
      0.0.toFloat
    }
}