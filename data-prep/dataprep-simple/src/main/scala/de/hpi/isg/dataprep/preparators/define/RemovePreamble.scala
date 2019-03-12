package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.cases.CharTypeVector
import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.implementation.DefaultRemovePreambleImpl
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  *
  * @author Justus Eilers, Theresia Bruns
  * @since 2018/29/15
  */
class RemovePreamble extends AbstractPreparator {


  this.impl = new DefaultRemovePreambleImpl

  /**
    * This method validates the input parameters of a [[AbstractPreparator]]. If succeeds, setup the values of metadata into both
    * prerequisite and toChange set.
    *
    * @throws Exception
    */
  override def buildMetadataSetup(): Unit = {
  }

  override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {

    //represents the chance the dataset doesnt have a preamble
    var finalScore = 1.0

    // does the dataframe have more than one column?
    finalScore *= RemovePreambleHelper.checkNumberOfColuns(dataset)

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

  def checkNumberOfColuns(dataFrame: DataFrame): Double = {
    val numberOfColumns = dataFrame.columns.length
    if(numberOfColumns == 1) {
      return 0.25
    }
    1
  }

  def calculateFirstCharOccurrence(dataFrame: DataFrame): Array[(Char, Int)] = {
    dataFrame
      .rdd
      .zipWithIndex()
      .map(e => (e._1.mkString("").charAt(0),List(e._2)))
      .reduceByKey(_.union(_))
      .flatMap(row => row._2.groupBy(k => k - row._2.indexOf(k)).toList.map(group => (row._1, group._2.size)))
      .filter(row => row._1.toString.matches("[^0-9a-zA-Z]"))
      .collect
  }

  def checkFirstCharacterInConsecutiveRows(dataset: Dataset[Row]): Double = {

    val charOccurence = calculateFirstCharOccurrence(dataset)

    if(charOccurence.isEmpty)
      return 1.0

    // score chsrs less if the occur often with different streaks
    val longestSeq = charOccurence.maxBy(a => a._2)
    val numberOfLongestSeq = charOccurence.count(e => e._2 == longestSeq._2)

    if(charOccurence.length == 1){
      return numberOfLongestSeq.toDouble/longestSeq._2
    }

    val secondSeq = charOccurence
      .filter(e => e._1 != longestSeq._1 || e._2 != longestSeq._2)
      .maxBy(a => a._2)


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
        .filter(r => !r.isEmpty)
        .map(row => (CharTypeVector.fromString(row.toString).simplify, 1))
        .groupByKey(_._1)
        .count()
        .count()

      maxTypes = Math.max(maxTypes,thisCount)
    }
    1.0/Math.pow(maxTypes.toDouble,2)
  }
}
