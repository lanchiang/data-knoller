package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.{PropertyDataType, PropertyDatePattern}

import scala.collection.JavaConverters._
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultAdaptiveChangeDateFormatImpl
import de.hpi.isg.dataprep.util.DataType
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.sql.{Dataset, Encoders, Row}

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
      if (alreadyApplied(targetMetadata)) {
        return 0
      }

      val preparator = new DefaultAdaptiveChangeDateFormatImpl
      var scores: Map[String, Float] = Map()
      val columnName = dataset.columns(0)

      val numTotalRows = dataset.count()
      // Approximate counting maybe better for runtime
      // val numTotalRows = dataset.rdd.countApprox(120000).getFinalValue().mean

      val samplePercentage = 0.1
      val sampleSizeOfRows = Math.ceil(numTotalRows * samplePercentage).toInt

      // Assume that 1000 rows can be checked in a reasonable time, so we don't need to abort early
      if (numTotalRows > 1000 && noMatchFor(dataset, preparator, sampleSizeOfRows)) {
        return 0
      }

      val numAppliedRows = dataset.rdd
        .map(_(0).toString)
        .map(x => preparator.toPattern(x))
        .filter(_.isDefined)
        .count()

      val score: Float = numAppliedRows.toFloat / numTotalRows
      println(s"$columnName: Number of applied Rows: $numAppliedRows, Rows total: $numTotalRows, Ratio/Score: $score")
      score
    }

    // Not ideal, as it will apply the Preparator.toPattern twice for sampleSizeOfRows
    // A sample is better to be sure that not just the first X rows are checked, but not used here
    // as it would make our test nondeterministic
    def noMatchFor(dataset: Dataset[Row], preparator: DefaultAdaptiveChangeDateFormatImpl, sampleSizeOfRows: Int): Boolean ={
      val countNotWorkingRows = dataset.rdd
        .take(sampleSizeOfRows)
        //.takeSample(false, sampleSizeOfRows)
        .map(_(0).toString)
        .map(x => preparator.toPattern(x))
        .filter(_.isDefined)
        .count(_ => true)

      countNotWorkingRows >= sampleSizeOfRows
    }

    def alreadyApplied(metadata: util.Collection[Metadata]): Boolean = {
      metadata.asScala.filter(m => m match {
        case _: PropertyDatePattern => return true
      })
      false
    }
}