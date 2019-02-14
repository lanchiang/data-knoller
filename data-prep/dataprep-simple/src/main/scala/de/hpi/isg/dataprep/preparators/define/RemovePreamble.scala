package de.hpi.isg.dataprep.preparators.define

import java.{lang, util}

import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata._
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

    //represents the chance the dataset doesnt have a preamble
    var finalScore = 1.0

    val numberOfColumns = dataset.columns.length
    if(numberOfColumns == 1)
    {
      finalScore *= 0.01
    }else{
      // dataset has one row, where there are missing values and they only occur in consecutive lines
      finalScore *= 1 - checkForConsecutiveEmptyRows(dataset)
    }

    // Consecutive lines starting with the same character
    finalScore *= 1 -  checkFirstCharacterInConsecutiveRows(dataset)
    // integrating split attribute?

    // number of consecutive lines a character doenst occur in but in all other lines does - even with same occurence count
    finalScore *= 1 - charsInEachLine(dataset)
    1 - finalScore.toFloat
  }

  def checkFirstCharacterInConsecutiveRows(dataset: Dataset[Row]): Double = {

    val charOccurence = dataset
      .rdd
      .zipWithIndex()
      .map(e => (e._1.toString().charAt(1),List(e._2)))
      .reduceByKey(_.union(_))
      .flatMap(row => row._2.groupBy(k => k - row._2.indexOf(k)).toList.map(group => (row._1, group._2.size)))
      .filter(row => row._1.toString.matches("[^0-9]"))
      .collect

    // score chsrs less if the occur often with different streaks
    val longestSeq = charOccurence.maxBy(a => a._2)
    val secondSeq = charOccurence.filter(e => e._2 < longestSeq._2).maxBy(a => a._2)
    val numberOfLongestSeq = charOccurence.count(e => e._2 == longestSeq._2)
    val decisionBound = (numberOfLongestSeq+secondSeq._2)/longestSeq._2
    decisionBound > 1 match {
      case true => 0.5
      case _ => 1 - decisionBound
    }
  }

  def checkForConsecutiveEmptyRows(dataset: Dataset[Row]): Double = {
    val emptyGroups = dataset
      .rdd
      .zipWithIndex()
      .flatMap(row => {
        val tempRow = row._1.toSeq.zipWithIndex.map(entry =>
          entry._1.toString match {
            case "" =>  List(entry._2)
            case _ => List()
          }).reduce((a,b) => a.union(b))
        tempRow.map(e => (e,row._2))
      })
      .map(e => (e._1,List(e._2)))
      .reduceByKey(_.union(_))
      .map(r => (r._1, r._2.groupBy(k => r._2.indexOf(k) - k)))
      .map(e => e._2.toList.map(v => v._2))
      .reduce(_.union(_))
      .map(l => l.size)
    val highestNumber = emptyGroups.max
    val maxCount = emptyGroups.count(e => e == highestNumber)
    val secondNumber = emptyGroups.filter(e => e < highestNumber).max
    val decisionBound = (maxCount+secondNumber)/highestNumber
    decisionBound > 1 match {
      case true => 0.5
      case _ => 1 - decisionBound
    }

  }

  def charsInEachLine(dataset: Dataset[Row]): Double = {
    val w2v = dataset
      .rdd
      .zipWithIndex()
      .map( e => (e._2, e._1.toString().toList.groupBy(e => e).toList.map(tup => (tup._1, tup._2.size))
      ))

    val reoccuringChars = w2v.fold(w2v.first())((tup1, tup2) => (tup2._1, tup1._2.intersect(tup2._2)))._2.size
    if(reoccuringChars == 0){
      return 1.0
    }
    0.0
  }
}
