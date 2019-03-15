package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.cases.CharTypeVector
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{DatasetError, PreparationError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.RemovePreambleHelper
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable
/**
  *
  * @author Justus Eilers, Theresia Bruns
  * @since 2018/11/29
  */
class DefaultRemovePreambleImpl extends AbstractPreparatorImpl {

  //ideas: average distance of characters in line

  override protected def executeLogic(abstractPreparator: AbstractPreparator,
                                      dataFrame_orig: Dataset[sql.Row],
                                      errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {


    var cleanedDataFrame = dataFrame_orig

    val firstCharMetric = RemovePreambleHelper.checkFirstCharacterInConsecutiveRows(dataFrame_orig)
    var reoccurringCharsMetric = RemovePreambleHelper.charsInEachLine(dataFrame_orig)

    var leadingCharAlgWasApplied = false

    // by leading character
    if(firstCharMetric * reoccurringCharsMetric < 0.25){
      cleanedDataFrame = analyseLeadingCharacter(cleanedDataFrame)
      leadingCharAlgWasApplied = true
    }


    val separatorChar = fetchSeparatorChar(cleanedDataFrame)

    // by separator occurrence
    val numberOfColumnsMetric = RemovePreambleHelper.checkNumberOfColuns(cleanedDataFrame)
    reoccurringCharsMetric = RemovePreambleHelper.charsInEachLine(cleanedDataFrame)
    val separatorSkewMetric = RemovePreambleHelper.calculateSeparatorSkew(cleanedDataFrame)

    val separatorIsNonAlphaNumeric = "[^A-Za-z0-9]".r.pattern.matcher(separatorChar).find()

    if(numberOfColumnsMetric * reoccurringCharsMetric * separatorSkewMetric < 0.125 && separatorIsNonAlphaNumeric && !leadingCharAlgWasApplied)
      cleanedDataFrame = analyseSeparatorCount(cleanedDataFrame, separatorChar.toString)


    val typeSkewMetric = RemovePreambleHelper.calculateCharacterTypeSkew(cleanedDataFrame)
    // by clustering
    if(typeSkewMetric < 0.5 && !leadingCharAlgWasApplied)
     cleanedDataFrame = findPreambleByTypesOfChars(cleanedDataFrame)

    if(cleanedDataFrame.count() == 0)
      errorAccumulator.add(new DatasetError)

    new ExecutionContext(cleanedDataFrame, errorAccumulator)
  }

  def fetchSeparatorChar(dataFrame: DataFrame): String = {
    import dataFrame.sparkSession.implicits._
    val stringedDataset = dataFrame.map {r => r.mkString("")}
    (new DefaultSplitPropertyImpl).findSeparator(stringedDataset).mostLikelySeparator.toList.head.toString
  }

  def analyseLeadingCharacter(dataframe:DataFrame): DataFrame = {
    val prominentChar = findPreambleChar(dataframe)

    dataframe.filter(r => r.mkString("").head.toString != prominentChar)
  }

  def findPreambleChar(dataframe:DataFrame): String = {
    import dataframe.sparkSession.implicits._
    val preambleChars = dataframe
      .map {r => r.mkString.charAt(0).toString}
      .filter(c => "[^A-Za-z0-9]".r.pattern.matcher(c).find())
      .groupBy("value")
      .count()
      .toDF("char", "count")

      if(preambleChars.count() > 0){
        preambleChars
          .reduce((a,b) => if (a.getLong(1) > b.getLong(1)) a else b )
          .getString(0)
      }else{
        ""
      }
  }

  def analyseSeparatorCount(dataFrame: DataFrame, separator:String): DataFrame = {

    val medianSepCount = findMedianSepCount(dataFrame, separator)
    dataFrame
      .filter(r => {
        val separatorCounts = separator.r.findAllIn(r.mkString("")).length.toDouble
        Math.abs(separatorCounts - medianSepCount) < 2
      })
  }

  def findPreambleByTypesOfChars(dataFrame: DataFrame): DataFrame = {

    val zippedDataFrame = dataFrame.rdd.zipWithIndex
    val collectorDataFrame = createDFforPreambleAnalysis(zippedDataFrame, dataFrame)
    val rowsToDelete = decideOnRowsToDelete(collectorDataFrame, identifyDataCluster(collectorDataFrame))

    val cleanRDD = zippedDataFrame
      .filter(r => !rowsToDelete.contains(r._2))
      .map(r => r._1)

    dataFrame.sqlContext.createDataFrame(cleanRDD, dataFrame.schema)
  }

  def dataFrameToCharTypeVectors(dataFrame: RDD[(Row, Long)], session:SparkSession): DataFrame = {
    import session.implicits._
    dataFrame
      .map(rowTuple =>
        (rowTuple._1
          .toSeq
          .map(e => if(e == null) "" else e)
          .map(e => CharTypeVector.fromString(e.toString).toDenseVector), rowTuple._2))
      .toDF("features", "rownumber")
  }

  def createEmptyThreeColumnDataFrame(dataFrame: DataFrame): DataFrame = {
    import dataFrame.sparkSession.implicits._
    Seq(Tuple3(Vectors.dense(1,2,3),1L, 3 )).toDF("a", "b", "c").filter(r => r.get(2).toString != "3")
  }

  def reduceDFtoSingleColumn(dataFrame: DataFrame, index:Int): DataFrame = {
    import dataFrame.sparkSession.implicits._
    dataFrame
      .map(row => (row.get(0).asInstanceOf[mutable.WrappedArray[DenseVector]](index),row.getAs[Long](1)))
      .toDF("features", "rownumber")
  }

  def kmeansForColumn(dataFrame: DataFrame): DataFrame = {
    val kmeans = new KMeans()
      .setK(2)
      .setSeed(1L)
      .setTol(0.1)
      .setInitMode("k-means||")
      .setInitSteps(3)
      .setDistanceMeasure("cosine")
    val model = kmeans.fit(dataFrame)

    model.transform(dataFrame)
  }

  def identifyDataCluster(dataFrame: DataFrame): Double = {
    dataFrame.stat.approxQuantile("c",Array(0.5),0.1).head
  }

  def decideOnRowsToDelete(dataFrame: DataFrame, dataCluster:Double): Array[Long] = {
    import dataFrame.sparkSession.implicits._
    // how many cols agree that a row is a preamble row
    val filteredSet = dataFrame
      .filter(r => r.getAs[Int](2) != dataCluster.toInt)
      .groupByKey(r => r.getAs[Long](1))
      .mapGroups((l, iter) => (l,iter.toList.length))

    if(filteredSet.isEmpty)
      return Array()

    // how many cols are there?
    val maxAgreeingCols = filteredSet.reduce((a,b) => if(a._2 > b._2) a else b)._2

    // how many rows should be allowed to vote - based on homogenety of result
    var maxHomogenetyScore = 0
    var optimalCols = 0
    for(agreeingCols <- (1 to maxAgreeingCols).inclusive){
      val preambleRows = filteredSet
        .filter(r => r._2 <= agreeingCols)
        .map(r => r._1)
        .collect

      val currentScore = homogenityOfList(preambleRows.toList)

      if(currentScore >= maxHomogenetyScore){
        optimalCols = agreeingCols
        maxHomogenetyScore = currentScore
      }
    }

    // row numbers of rows that are part of the preamble
    filteredSet
      .filter(r => r._2 >= optimalCols)
      .map(r => r._1)
      .collect
  }

  def homogenityOfList(rowIdicies:List[Long] ): Int = {

    val sortedList = rowIdicies
      .sorted

    var lastEntry:Long = -1
    var score = 0

    for(entry:Long <- sortedList){
      if(lastEntry != -1){
        if((entry - lastEntry) == 1){
          score += 1
        }else{
          val penalty = Math.pow(entry - lastEntry, 2)/2
          if(penalty <= score){
            score = score - penalty.toInt
          }
        }
      }
      lastEntry = entry
    }
    score
  }

  def createDFforPreambleAnalysis(zippedDataFrame: RDD[(Row, Long)], origDataFrame: DataFrame): DataFrame = {

    val charTypeCounts = dataFrameToCharTypeVectors(zippedDataFrame, origDataFrame.sparkSession)
    var collectorDataFrame = createEmptyThreeColumnDataFrame(origDataFrame)

    for(col <- origDataFrame.columns.indices){
      val oneColumnDataframe = reduceDFtoSingleColumn(charTypeCounts, col)
      val colResult = kmeansForColumn(oneColumnDataframe)
      collectorDataFrame = collectorDataFrame.union(colResult)
    }
    collectorDataFrame
  }

  def findMedianSepCount(dataFrame: DataFrame, separator:String): Double = {
    import dataFrame.sparkSession.implicits._
    val sepCountFrame = dataFrame
      .map {r => r.mkString("")}
      .map {r => separator.r.findAllIn(r).length}

    sepCountFrame
      .toDF("sepCount")
      .stat
      .approxQuantile("sepCount",Array(0.5),0.01)
      .head
  }
}
