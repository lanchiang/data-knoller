package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.cases.CharTypeVector
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata._
import de.hpi.isg.dataprep.model.target.objects.{FileMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.schema.{Schema, SchemaMapping}
import de.hpi.isg.dataprep.preparators.implementation.DefaultRemovePreambleImpl
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  *
  * @author Justus Eilers, Theresia Bruns
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

    //represents the chance the dataset doesnt have a preamble
    var finalScore = 1.0

    val numberOfColumns = dataset.columns.length
    if(numberOfColumns == 1) {
      finalScore *= 0.01
    }

    // Consecutive lines starting with the same character
    finalScore *= RemovePreambleHelper.checkFirstCharacterInConsecutiveRows(dataset)
    // integrating split attribute?

    // number of consecutive lines a character doenst occur in but in all other lines does - even with same occurence count
    finalScore *= RemovePreambleHelper.charsInEachLine(dataset)

    //high separator occurrence skew
    finalScore *= RemovePreambleHelper.calculateSeparatorSkew(dataset)

    //vastly different char types
    finalScore *= RemovePreambleHelper.calculateCharacterTypeSkew(dataset)

    1 - finalScore.toFloat
  }
}

object RemovePreambleHelper {

  def calculateFirstCharOccurrence(dataFrame: DataFrame): Array[(Char, Int)] = {
    dataFrame
      .rdd
      .zipWithIndex()
      .map(e => (e._1.mkString("").charAt(0),List(e._2)))
      .reduceByKey(_.union(_))
      .flatMap(row => row._2.groupBy(k => k - row._2.indexOf(k)).toList.map(group => (row._1, group._2.size)))
      .filter(row => row._1.toString.matches("[^0-9]"))
      .collect
  }

  def checkFirstCharacterInConsecutiveRows(dataset: Dataset[Row]): Double = {

    val charOccurence = calculateFirstCharOccurrence(dataset)

    // score chsrs less if the occur often with different streaks
    val longestSeq = charOccurence.maxBy(a => a._2)
    if(charOccurence.length == 1){
      return 0.0
    }
    val secondSeq= charOccurence
      .filter(e => e != longestSeq)
      .maxBy(a => a._2)
    val numberOfLongestSeq = charOccurence.count(e => e._2 == longestSeq._2)
    val decisionBound = longestSeq._2/(numberOfLongestSeq + secondSeq._2)

    decisionBound > 1 match {
      case false => 1
      case _ => 1.0/decisionBound.toDouble
    }
  }

  def charsInEachLine(dataset: Dataset[Row]): Double = {
    val w2v = dataset
      .rdd
      .zipWithIndex()
      .map( e => (e._2, e._1.mkString("").toList.groupBy(e => e).toList.map(tup => (tup._1, tup._2.size))
      ))

    val reoccuringChars = w2v.fold(w2v.first())((tup1, tup2) => (tup2._1, tup1._2.intersect(tup2._2)))._2.size
    if(reoccuringChars == 0){
      return 0.0
    }
    1.0
  }

  def calculateSeparatorSkew(dataFrame: DataFrame): Double = {
    import dataFrame.sparkSession.implicits._
    val separator = (new DefaultRemovePreambleImpl).fetchSeparatorChar(dataFrame)

    val sepCountFrame = dataFrame
        .rdd
        .zipWithIndex
      .map(r => (separator.r.findAllIn(r._1.mkString("")).length, r._2))
        .toDF("val", "index")

    val median = sepCountFrame.stat.approxQuantile("val", Array(0.5), 0.1).head

    val unusualRows = sepCountFrame
      .filter(_.getAs[Int](0) != median)
      .collect

    if(unusualRows.length == 0){
      return 1.0
    }

    val longestUnusualStreak = unusualRows
      .groupBy(r => r.getAs[Long](1) - unusualRows.indexOf(r))
      .mapValues(_.length)
      .values
      .max

    1.0/longestUnusualStreak.toDouble
  }

  def calculateCharacterTypeSkew(dataFrame: DataFrame): Double = {
    import dataFrame.sparkSession.implicits._

    var maxTypes = 0L

    for(col <- dataFrame.columns){
      val thisCount = dataFrame
        .map(_.getAs[String](col))
        .map(row => (CharTypeVector.fromString(row.toString).simplify,1))
        .groupByKey(_._1)
        .count()
        .count()

      maxTypes = Math.max(maxTypes,thisCount)
    }
    1.0/Math.pow(maxTypes.toDouble,2)
  }
}
