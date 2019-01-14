package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata._
import de.hpi.isg.dataprep.model.target.data.ColumnCombination
import de.hpi.isg.dataprep.model.target.objects.{FileMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.preparators.implementation.DefaultRemovePreambleImpl
import org.apache.spark.sql.{Dataset, Row}

/**
  *
  * @author Lasse Kohlmeyer
  * @since 2018/29/15
  */
class RemovePreamble(val delimiter: String, val hasHeader: String, val hasPreamble: Boolean, val rowsToRemove: Integer, val commentCharacter: String) extends AbstractPreparator {

  def this(delimiter: String, hasHeader: String, rowsToRemove: Integer) = this(delimiter, hasHeader, true, rowsToRemove, "")

  def this(delimiter: String, hasHeader: String, commentCharacter: String) = this(delimiter, hasHeader, true, 0, commentCharacter)

  def this(delimiter: String, hasHeader: String) = this(delimiter, hasHeader, true, 0, "")

  def this(delimiter: String, hasHeader: String, hasPreamble: Boolean, rowsToRemove: Integer) = this(delimiter, hasHeader, hasPreamble, rowsToRemove, "")

  def this(delimiter: String, hasHeader: String, hasPreamble: Boolean, commentCharacter: String) = this(delimiter, hasHeader, hasPreamble, 0, commentCharacter)

  def this(delimiter: String, hasHeader: String, hasPreamble: Boolean) = this(delimiter, hasHeader, hasPreamble, 0, "")


  this.impl = new DefaultRemovePreambleImpl

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
    val prerequisites = new util.ArrayList[Metadata]
    val tochanges = new util.ArrayList[Metadata]

    if (delimiter == null) throw new ParameterNotSpecifiedException(String.format("Delimiter not specified."))
    if (hasHeader == null) throw new ParameterNotSpecifiedException(String.format("No information about header"))

    prerequisites.add(new CommentCharacter(delimiter, new FileMetadata("")))
    prerequisites.add(new RowsToRemove(-1, new FileMetadata("")))
    prerequisites.add(new Delimiter(delimiter, new FileMetadata("")))
    prerequisites.add(new HeaderExistence(hasHeader.toBoolean, new FileMetadata("")))
    prerequisites.add(new PreambleExistence(true))
    tochanges.add(new PreambleExistence(false))

    this.prerequisites.addAll(prerequisites)
    this.updates.addAll(tochanges)
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    // what speaks for having a preamble?
    // Dataset only has one row
    var finalScore = 0.0

    val numberOfColumns = dataset.columns.length
    if(numberOfColumns == 1)
    {
      finalScore += 0.9
    }else{
      finalScore += checkForConsecutiveEmptyRows(dataset)
    }
    // dataset has one row, where there are missing values and they only occur in consecutive lines
    // Consecutive lines starting with the same character
    // integrating split attribute?
    // number of consecutive lines a character doenst occur in but in all other lines does - even with same occurence count
    if(finalScore > 1.0) {
      1.0F
    }else{
      finalScore.toFloat
    }
  }
  def checkForConsecutiveEmptyRows(dataset: Dataset[Row]): Double = {
    dataset
      .rdd
      .zipWithIndex()
      .foreach(println(_))
    0.0
  }
}
