package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.preparators.define.{ChangeDateFormat, RemovePreambleHelper}
import de.hpi.isg.dataprep.preparators.implementation.{DateRegex, DefaultChangeDateFormatImpl}
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import de.hpi.isg.dataprep.preparators.implementation.DefaultRemovePreambleImpl
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable

class RemovePreamble extends PreparatorScalaTest {

  var sparkContext:SparkSession = _

  override def beforeAll: Unit = {
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]")
    sparkContext = sparkBuilder.getOrCreate()
    super.beforeAll()
  }

  "Spark should be able to read files" should "work" in {
    val localContext = sparkContext
    val fileData = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/test_space_preamble.csv")

    fileData.columns.length shouldEqual 3
  }

  "Initial char" should "find right counts for each char" in {
    val localContext = sparkContext
    import localContext.implicits._
    val customDataset = Seq(("1","2"), ("3","4"), ("#postamble",""),("#postamble",""),("#postamble",""), ("","fake")).toDF

    val prep = new DefaultRemovePreambleImpl

    val result = prep.findPreambleChar(customDataset)

    result shouldEqual "#"
  }

  "Initial char" should "remove all preamble lines starting with #" in {
    val localContext = sparkContext
    val customDataset = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_initial_char.csv")

    val expectedDataset = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_initial_char_expected.csv")

    val prep = new DefaultRemovePreambleImpl
    val result = prep.analyseLeadingCharacter(customDataset)

    RemovePreambleHelper.checkFirstCharacterInConsecutiveRows(customDataset) shouldEqual 0.0

    result.collect shouldEqual expectedDataset.collect
  }


  "Initial char" should "remove # lines but not ." in {
    val localContext = sparkContext
    val customDataset = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_initial_char_fail.csv")

    val expectedDataset = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_initial_char_fail_expected.csv")

    val prep = new DefaultRemovePreambleImpl
    val result = prep.analyseLeadingCharacter(customDataset)

    //RemovePreambleHelper.calculateFirstCharOccurrence(customDataset) shouldEqual List(1)
    RemovePreambleHelper.checkFirstCharacterInConsecutiveRows(customDataset) shouldEqual 1.0

    result.collect shouldEqual expectedDataset.collect
  }

  "Initial char with space" should "remove # lines with space" in {
    val localContext = sparkContext
    import localContext.implicits._
    val fileData = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_initial_char_space_fail.csv")
    val customDataset = fileData

    val fileDataExpected = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_initial_char_space_fail_expected.csv")
    val expectedDataset = fileDataExpected

    val prep = new DefaultRemovePreambleImpl
    val result = prep.analyseLeadingCharacter(customDataset)

    result.collect shouldEqual expectedDataset.collect
  }

  "Separator Count" should "remove all preamble lines with an unusual amount of comma separators" in {
    val localContext = sparkContext
    import localContext.implicits._
    val prep = new DefaultRemovePreambleImpl
    val customDataset = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_separator_comma.csv")

    prep.fetchSeparatorChar(customDataset) shouldEqual ","

    RemovePreambleHelper.charsInEachLine(customDataset) shouldEqual 0.0

    RemovePreambleHelper.calculateSeparatorSkew(customDataset) shouldEqual 2

    val expectedDataset = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_separator_comma_expected.csv")



    val result = prep.analyseSeparatorCount(customDataset, separator = ",")



    result.collect shouldEqual expectedDataset.collect
  }

  "Separator Count" should "remove all preamble lines with an unusual amount of slash separators" in {
    val localContext = sparkContext
    import localContext.implicits._

    val customDataset = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_separator_slash.csv")

    val expectedDataset = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_separator_slash_expected.csv")

    val prep = new DefaultRemovePreambleImpl

    prep.fetchSeparatorChar(customDataset) shouldEqual "/"

    val result = prep.analyseSeparatorCount(customDataset, separator = "/")

    result.collect shouldEqual expectedDataset.collect
  }

  "Separator Count" should "fail with an unusual amount of comma separators" in {
    val localContext = sparkContext
    import localContext.implicits._

    val fileData = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_separator_fail.csv")
    val customDataset = fileData

    val fileDataExpected = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/preamble_separator_fail_expected.csv")
    val expectedDataset = fileDataExpected

    val prep = new DefaultRemovePreambleImpl

    val result = prep.analyseSeparatorCount(customDataset, separator = ",")

    result.collect shouldEqual expectedDataset.collect
  }

  "CharTypeClusterer" should "generate Vectors correctly" in {
    val localContext = sparkContext
    val fileData = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/test_space_preamble.csv")
    val customDataset = fileData

    val testPreparator = new DefaultRemovePreambleImpl

    val zippedDataset = testPreparator.findPreambleByTypesOfChars(customDataset)

    zippedDataset.filter(row => row.get(2) == 1).collect.length shouldEqual 15
  }

  "CharTypeClusterer" should "find Preamble Cluster on its own" in {
    val localContext = sparkContext
    val fileData = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/test_space_preamble.csv")
    val customDataset = fileData

    val testPreparator = new DefaultRemovePreambleImpl

    val result = testPreparator.findPreambleByTypesOfChars(customDataset)
    val cluster = testPreparator.identifyDataCluster(result)
    result.filter(r => r.getAs[Int](2) == cluster.toInt).collect().length shouldEqual 90
  }

  "CharTypeClusterer" should "use most homogeneous column for preamble detection" in {
    val localContext = sparkContext
    val fileData = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/test_space_preamble.csv")
    val customDataset = fileData.rdd.zipWithIndex

    val testPreparator = new DefaultRemovePreambleImpl

    val result = testPreparator.createDFforPreambleAnalysis(customDataset, fileData)
    val cluster = testPreparator.identifyDataCluster(result)

    testPreparator.decideOnRowsToDelete(result, cluster).length shouldEqual 5
  }

  "Homogenety of List" should "give a good indication of how close together lines are" in {
    val homogenList = List(1,2,3,4,5,6).map(_.toLong)
    val unhomogenList = List(1,3,4,5,7,10,20).map(_.toLong)
    val homogenButSplitList = List(1,2,3,30,31,32).map(_.toLong)

    val testPreparator = new DefaultRemovePreambleImpl

    val homogenValue = testPreparator.homogenityOfList(homogenList)
    val unhomogenValue = testPreparator.homogenityOfList(unhomogenList)
    val partialHomogenValue = testPreparator.homogenityOfList(homogenButSplitList)

    homogenValue shouldEqual 5
    partialHomogenValue shouldEqual 4
    unhomogenValue shouldEqual 0
  }

  "CharTypeClusterer" should "remove preamble" in {
    val localContext = sparkContext
    val fileData = localContext.read
      .option("sep", "\t")
      .csv("../dataprep-simple/src/test/resources/test_space_preamble.csv")
    val customDataset = fileData

    val testPreparator = new DefaultRemovePreambleImpl

    val result = testPreparator.findPreambleByTypesOfChars(customDataset)

    result.collect.length shouldEqual 30
  }
}