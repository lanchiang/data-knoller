package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.PreparationError
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.RemovePreamble
import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.cases.CharTypeVector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql._
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.ml.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{DenseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.catalyst.encoders.RowEncoder

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

    import dataFrame_orig.sparkSession.implicits._
    val stringedDataset = dataFrame_orig.map {r => r.mkString("")}
    val separatorChar = (new DefaultSplitPropertyImpl).findSeparator(stringedDataset, stringedDataset.columns.length)

    // by leading character
    val leadingCharRemovedDataset = analyseLeadingCharacter(dataFrame_orig)

    // by separator occurrence
    val unexpectedSepAmountRemovedDataset = analyseSeparatorCount(dataFrame_orig, separatorChar.toString)

    // by clustering
    val unequalCharTypesRemovedDataset = findPreambleChar(dataFrame_orig)

    new ExecutionContext(dataFrame_orig, errorAccumulator)
  }

  def analyseLeadingCharacter(dataframe:DataFrame): DataFrame = {
    val prominentChar = findPreambleChar(dataframe)

    dataframe
      .filter(r => r.mkString("").head.toString != prominentChar)
  }

  def findPreambleChar(dataframe:DataFrame): String = {
    import dataframe.sparkSession.implicits._
    dataframe
      .map {r => r.mkString.charAt(0).toString}
      .filter(c => !"[A-Za-z0-9]".r.pattern.matcher(c).find())
      .groupBy("value")
      .count()
      .toDF("char", "count")
      .reduce((a,b) => if (a.getLong(1) > b.getLong(1)) a else b )
      .getString(0)
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
      .map(rowTuple => (rowTuple._1.toSeq.map(e => CharTypeVector.fromString(e.toString).toDenseVector), rowTuple._2))
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

  //TODO: refactor, delete useless stuff



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

  def outlierWordToVec(dataframe:DataFrame): DataFrame = {
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]")
    val sparkContext = sparkBuilder.getOrCreate()
    import sparkContext.implicits._

    val zippedDataset = dataframe
      .rdd
      .zipWithIndex()
      .flatMap(row => row._1.toSeq.map( v => (v.toString.split(""), row._2, row._1.toSeq.indexOf(v))))
      .toDF("value", "line", "column")

    for(columnIndex <- dataframe.columns.indices){
      findPreableForColumn(zippedDataset.filter(r => r.getInt(2) == columnIndex), sparkContext)
    }
    dataframe
  }

  def findPreableForColumn(dataframe:DataFrame, sparkContext: SparkSession): DataFrame  = {

    import sparkContext.implicits._

    val vocabSize = dataframe
      .map(r => r.getAs[mutable.WrappedArray[String]]("value").head.split("").toList)
      .reduce(_.union(_))
      .toSet
      .size

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("value")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .setMinDF((dataframe.count()*0.1).toInt)
      .fit(dataframe)

    val result = cvModel.transform(dataframe)
    val bkm = new KMeans().setK(2).setSeed(1)  //new BisectingKMeans().setK(2).setSeed(1)
    val modelBM = bkm.fit(result)

    val clusteredVecs = modelBM
      .transform(result)
      .toDF
      .rdd
      .map { r =>
        (r.getAs[mutable.WrappedArray[String]](0), r.getLong(1), r.getAs[Int](2), r.getInt(4))
      }
      .persist
      .toDF("value","line", "column", "cluster")

    val largestCluster = clusteredVecs.stat.approxQuantile("cluster",Array(0.5),0.1).head

    clusteredVecs.filter(r => r.getAs("cluster") != largestCluster)
  }

  def findPreambleByClustering(dataframe: DataFrame,  separator: String): DataFrame ={

    val (sparkContext: SparkSession, separatorOcc: DataFrame) = createBuilderAndFindSeparator(dataframe, separator)
    import sparkContext.implicits._
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(separatorOcc)

    val predictions = model.transform(separatorOcc)

    val maxCluster = predictions
      .groupBy("prediction")
      .count()
      .collect
      .maxBy(row => row.getLong(1))
      .getInt(0)

    val filteredLines = predictions
      .filter(row => row.getAs("prediction") == maxCluster)
      .select("rownumber")
      .map(row => row.getLong(0))
      .collect

    val resultRDD = dataframe
      .rdd.zipWithIndex()
      .filter(row => filteredLines contains row._2)
      .map(row => row._1)
      .persist

    sparkContext.createDataFrame(resultRDD, dataframe.schema)
  }


  def findPreambleByMedian(dataframe: DataFrame, separator: String): DataFrame ={

    val (sparkContext: SparkSession, separatorOcc: DataFrame) = createBuilderAndFindSeparator(dataframe, separator)

    val kmeans = new KMeans().setK(2).setSeed(1L).fit(separatorOcc)

    val predictions = kmeans.transform(separatorOcc)

    val maxCluster = predictions
      .groupBy("prediction")
      .count()
      .collect
      .maxBy(row => row.getLong(1))
      .getInt(0)

    //get List of Int cluster
/*
   val rdd: RDD[Int] = sc.parallelize((1), (2), (5), (3), (2), (4))

    val median = calculateMedian(rdd)

    val filteredLinesByMedian = predictions
      .filter(row => row.getAs("prediction") == median)
      .select("rownumber")
      .map(row => row.getLong(0))
      .collect

    val resultRDD = dataframe
      .rdd.zipWithIndex()
      .filter(row => filteredLinesByMedian contains row._2)
      .map(row => row._1)
      .persist

    sparkContext.createDataFrame(resultRDD, dataframe.schema)*/
    dataframe
  }

  private def calculateMedian(rdd: RDD[Int]) = {
    val sorted = rdd.sortBy(identity).zipWithIndex().map {
      case (v, idx) => (idx, v)
    }
    val count = sorted.count()

    val median: Double = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (sorted.lookup(l).head + sorted.lookup(r).head).toDouble / 2
    } else sorted.lookup(count / 2).head.toDouble
  }

  private def createBuilderAndFindSeparator(dataframe: DataFrame, separator: String) = {
    val sparkBuilder = SparkSession
      .builder()
      .master("local[4]")
    val sparkContext = sparkBuilder.getOrCreate()
    import sparkContext.implicits._

    val separatorOcc = dataframe
      .rdd
      .zipWithIndex()
      .map(row => (Vectors.dense(separator.r.findAllIn(row._1.mkString).length), row._2))
      .toDF("features", "rownumber")
    (sparkContext, separatorOcc)
  }


  /**
    * The abstract class of preparator implementation.
    *
    * @param abstractPreparator is the instance of { @link AbstractPreparator}. It needs to be converted to the corresponding subclass in the implementation body.
    * @param dataFrame        contains the intermediate dataset
    * @param errorAccumulator is the { @link CollectionAccumulator} to store preparation errors while executing the preparator.
    * @return an instance of { @link ExecutionContext} that includes the new dataset, and produced errors.
    * @throws Exception
    */
}
