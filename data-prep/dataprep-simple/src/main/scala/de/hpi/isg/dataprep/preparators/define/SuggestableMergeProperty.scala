package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator.PreparatorTarget
import org.apache.spark.sql.{Dataset, Row}

import scala.util.{Failure, Success, Try}

/**
  * Suggestable merge property preparator.
  *
  * @author Lan Jiang
  * @since 2019-04-17
  */
class SuggestableMergeProperty(var attributes: List[String], var connector: String = null) extends AbstractPreparator {

  def this() {
    this(null)
  }

  preparatorTarget = PreparatorTarget.COLUMN_BASED

  /** *  THRESHOLDS FOR calApplicability  ***/
  val threshold = 0.95

  override def buildMetadataSetup(): Unit = ???

  /**
    * Calculates calApplicability for columns to merge.
    * Expects 2 columns for normal merge and 3 columns for date-parts-merge.
    * If more than 3 columns are given, returns 0.
    *
    * @param schemaMapping  is the schema of the input data towards the schema of the output data.
    * @param dataset        is the input dataset slice. A slice can be a subset of the columns of the data,
    *                       or a subset of the rows of the data.
    * @param targetMetadata is the set of { @link Metadata} that shall be fulfilled for the output data
    **/
  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    val columns = dataset.columns.toSeq
    // Todo: column combination size can be more than 2

    val score = dataset.columns.length match {
      case 0 => 0 // the dataset is empty Todo: maybe an exception should be thrown.
      case 1 => 0 // interleaving does not work for only one column
      case 2 => {
        calApplicabilityInterleave(dataset, targetMetadata) // calculate the interleaving score
      }
      case 3 => {
        callApplicabilityDate(schemaMapping, dataset, targetMetadata) // calculate the merging date score
      }
      case _ => 0 // currently interleaving does not support more than three columns.
    }

    // fill the parameters
    this.attributes = columns.toList
    score
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Interleaving columns handling                                                                                  //
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * This function calculates the applicability score of two columns regarding whether they are interleaving each other and should be merged.
    *
    * @param dataset is the input dataset slice. A dataset slice is a column subset, or row subset of the original dataset.
    *                The input of this dataset can only be two column Dataset.
    * @param targetMetadata is the set of [[Metadata]] that shall be fulfilled for the output data
    * @return a score between zero and one inclusively, indicating the how possible these two columns are interleaving each other.
    */
  private def calApplicabilityInterleave(dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {

    // calculate the interleaving degree score
    dataset.columns.size match {
      case 2 => {
        // for each row, one and only one cell is null
        val OnlyOneNoneScore = dataset.filter(row => {
          // now use spark's built-in null checker
          !row.isNullAt(0).equals(row.isNullAt(1))
        }).count / dataset.count.toFloat

        // values of the two columns should be semantically similar
        val dataTypeArray = dataset.dtypes.unzip._2
        // check whether two columns have the same data type
        val sameDataTypeScore = dataTypeArray(0).equals(dataTypeArray(1)) match {
          case true => 1
          case false => 0 // different data types indicate column heterogeneity, and therefore these two columns are not interleaving each other.
        }

        // Todo: inspect the content semantics. Now only data type is considered.
        // Todo: Idea: measure the distribution of values after merging these two columns.

        OnlyOneNoneScore * sameDataTypeScore.toFloat
      }
      case _ => throw new RuntimeException("The input dataset does not contain 2 columns.")
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Date handling.                                                                                                 //
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /** *
    * This function calculates the callApplicability assuming we want to merge date parts (year, month, day).
    * First the column names are checked, if no match was found, the rows are checked to be in value ranges, that
    * would make sense for dates.
    *
    * @param schemaMapping  is the schema of the input data towards the schema of the output data.
    * @param dataset        is the input dataset slice. A slice can be a subset of the columns of the data,
    *                       or a subset of the rows of the data.
    * @param targetMetadata is the set of { @link Metadata} that shall be fulfilled for the output data
    * @return 0 if list of rows is not (day, month, year) or more or less than 3 columns are given. returns 1 if date if found
    */
  def callApplicabilityDate(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
    val columns = dataset.columns.toSeq
    if (columns.length != 3)
      return 0

    this.attributes = columns.toList.reverse
    this.connector = "."

    //check headers
    if (headerIsDay(columns(2)) && headerIsMonth(columns(1)) && headerIsYear(columns(0))) {
      return 1
    }
    //check rows
    else if (isDay(dataset, columns(2)) && isMonth(dataset, columns(1)) && isYear(dataset, columns(0))) {
      return 1
    }
    else
      return 0
  }

  /** *
    * Checks if header to find year column.
    *
    * @param header column name
    * @return true if column is year
    */
  def headerIsYear(header: String): Boolean = if (header.toLowerCase.contains("year")) true else false

  /** *
    * Checks if header to find month column.
    *
    * @param header column name
    * @return true if column is month
    */
  def headerIsMonth(header: String): Boolean = if (header.toLowerCase.contains("month")) true else false

  /** *
    * Checks if header to find day column.
    *
    * @param header column name
    * @return true if column is day
    */
  def headerIsDay(header: String): Boolean = if (header.toLowerCase.contains("day")) true else false

  /** *
    * Check column contents to check if it contains the year part of a date.
    *
    * @param dataset    dataset to find year in
    * @param columnName name of column to inspect
    * @return true if column is year
    */
  def isYear(dataset: Dataset[Row], columnName: String): Boolean = {
    import dataset.sparkSession.implicits._
    Try {
      val columnWithoutNull = getColumnsWithoutNull(dataset, columnName)
      val sumYear = columnWithoutNull
              .map(x => if (x <= 2100 && x > 0) 1 else 0)
              .reduce(_ + _)
      sumYear / columnWithoutNull.count() >= threshold
    } match {
      case Success(value) => value
      case Failure(_) => return false
    }

  }

  /** *
    * Check column contents to check if it contains the month part of a date.
    *
    * @param dataset    dataset to find month in
    * @param columnName name of column to inspect
    * @return true if column is month
    */
  def isMonth(dataset: Dataset[Row], columnName: String): Boolean = {
    import dataset.sparkSession.implicits._
    Try{
      val columnWithoutNull = getColumnsWithoutNull(dataset, columnName)
      val sumMonth = columnWithoutNull
              .map(x => if (x <= 12 && x > 0) 1 else 0)
              .reduce(_ + _)
      sumMonth / columnWithoutNull.count() >= threshold
    } match {
      case Success(value) => value
      case Failure(_) => false
    }

  }

  /** *
    * Check column contents to check if it contains the day part of a date.
    *
    * @param dataset    dataset to find day in
    * @param columnName name of column to inspect
    * @return true if column is day
    */
  def isDay(dataset: Dataset[Row], columnName: String): Boolean = {
    import dataset.sparkSession.implicits._
    Try{
      val columnWithoutNull = getColumnsWithoutNull(dataset, columnName)
      val sumDay = columnWithoutNull
              .map(x => if (x <= 31 && x > 0) 1 else 0)
              .reduce(_ + _)
      sumDay / columnWithoutNull.count() >= threshold
    } match {
      case Success(value) => value
      case Failure(_) => false
    }

  }

  /***
    * Return a dataset with only the given column, all null rows removed and converted to int.
    * @param dataset input dataset
    * @param columnName name of the column to keep
    * @return	dataset[Int] none null rows in the given column
    */
  def getColumnsWithoutNull(dataset: Dataset[Row], columnName: String): Dataset[Int] = {
    import dataset.sparkSession.implicits._
    dataset.select(columnName)
            .filter(x => {
              val str = if (x.isNullAt(0)) null else x.get(0).toString
              !MergePropertiesUtil.isNull(str)
            })
            .map(x => {
              Try{x.getString(0).toInt} match {
                case Success(value) => value
                case Failure(exception) => throw exception
              }
            })
  }

  override def toString = s"SuggestableMergeProperty($attributes, $connector)"
}
