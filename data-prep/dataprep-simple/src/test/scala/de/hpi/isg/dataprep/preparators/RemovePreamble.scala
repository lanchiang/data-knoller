package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.preparators.define.RemovePreambleHelper
import de.hpi.isg.dataprep.preparators.implementation.DefaultRemovePreambleImpl
import org.apache.spark.sql.SparkSession

class RemovePreamble extends PreparatorScalaTest {
  var sparkContext:SparkSession = _

  override var resourcePath: String = "/pokemon.csv"

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
      .csv("./src/test/resources/test_space_preamble.csv")

    fileData.columns.length shouldEqual 1
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
      .csv("./src/test/resources/preamble_initial_char.csv")

    val expectedDataset = localContext.read
      .option("sep", "\t")
      .csv("./src/test/resources/preamble_initial_char_expected.csv")

    val prep = new DefaultRemovePreambleImpl
    val result = prep.analyseLeadingCharacter(customDataset)

    RemovePreambleHelper.checkFirstCharacterInConsecutiveRows(customDataset) shouldEqual (1.0/3.0)

    result.collect shouldEqual expectedDataset.collect
  }

  // this test fails when the number of some other leading non-alphanumeric characters is the same as the one expected.
  // then the result is non-determined.
  "Initial char" should "remove # lines but not ." in {
    val localContext = sparkContext
    val customDataset = localContext.read
      .option("sep", "\t")
            .csv(getClass.getClassLoader.getResource("preamble_initial_char_fail.csv").toString)

    val expectedDataset = localContext.read
      .option("sep", "\t")
            .csv(getClass.getClassLoader.getResource("preamble_initial_char_fail_expected.csv").toString)

    val prep = new DefaultRemovePreambleImpl
    val result = prep.analyseLeadingCharacter(customDataset)

    RemovePreambleHelper.checkFirstCharacterInConsecutiveRows(customDataset) shouldEqual 1.0

    result.collect shouldEqual expectedDataset.collect
  }

  "Initial char with space" should "remove # lines with space" in {
    val localContext = sparkContext
    val customDataset = localContext.read
      .option("sep", "\t")
            .csv(getClass.getClassLoader.getResource("preamble_initial_char_space_fail.csv").toString)

    val expectedDataset = localContext.read
      .option("sep", "\t")
            .csv(getClass.getClassLoader.getResource("preamble_initial_char_space_fail_expected.csv").toString)

    val prep = new DefaultRemovePreambleImpl
    val result = prep.analyseLeadingCharacter(customDataset)

    result.collect shouldEqual expectedDataset.collect
  }

  "Separator Count" should "remove all preamble lines with an unusual amount of comma separators" in {
    val localContext = sparkContext
    val prep = new DefaultRemovePreambleImpl
    val customDataset = localContext.read
      .option("sep", "\t")
      .csv("./src/test/resources/preamble_separator_comma.csv")

    prep.fetchSeparatorChar(customDataset) shouldEqual ","

    RemovePreambleHelper.charsInEachLine(customDataset) shouldEqual 0.0
    RemovePreambleHelper.calculateSeparatorSkew(customDataset) shouldEqual 0.5

    val expectedDataset = localContext.read
      .option("sep", "\t")
      .csv("./src/test/resources/preamble_separator_comma_expected.csv")

    val result = prep.analyseSeparatorCount(customDataset, separator = ",")
    result.collect shouldEqual expectedDataset.collect
  }

  "Separator Count" should "remove all preamble lines with an unusual amount of slash separators" in {
    val localContext = sparkContext

    val customDataset = localContext.read
      .option("sep", "\t")
      .csv("./src/test/resources/preamble_separator_slash.csv")

    val expectedDataset = localContext.read
      .option("sep", "\t")
      .csv("./src/test/resources/preamble_separator_slash_expected.csv")

    val prep = new DefaultRemovePreambleImpl

    prep.fetchSeparatorChar(customDataset) shouldEqual "/"

    val result = prep.analyseSeparatorCount(customDataset, separator = "/")

    result.collect shouldEqual expectedDataset.collect
  }

  "Separator Count" should "fail with an unusual amount of comma separators" in {
    val localContext = sparkContext

    val customDataset = localContext.read
      .option("sep", "\t")
      .csv("./src/test/resources/preamble_separator_fail.csv")

    val expectedDataset = localContext.read
      .option("sep", "\t")
      .csv("./src/test/resources/preamble_separator_fail_expected.csv")

    val prep = new DefaultRemovePreambleImpl

    val result = prep.analyseSeparatorCount(customDataset, separator = ",")

    result.collect shouldEqual expectedDataset.collect
  }

  "CharTypeClusterer" should "find correct rows to delete" in {
    val localContext = sparkContext
    val customDataset = localContext.read
      .option("sep", " ")
      .csv("./src/test/resources/test_space_preamble.csv")

    val prep = new DefaultRemovePreambleImpl

    val zippedDataFrame = customDataset.rdd.zipWithIndex
    val collectorDataFrame = prep.createDFforPreambleAnalysis(zippedDataFrame, customDataset)

    val rowsToDelete = prep.decideOnRowsToDelete(collectorDataFrame, prep.identifyDataCluster(collectorDataFrame))

    rowsToDelete.sorted shouldEqual Array(0,1,2,3,4)
  }

  "CharTypeClusterer" should "remove preamble" in {
    val localContext = sparkContext
    val customDataset = localContext.read
      .option("sep", "\t")
      .csv("./src/test/resources/test_space_preamble.csv")

    val testPreparator = new DefaultRemovePreambleImpl

    val result = testPreparator.findPreambleByTypesOfChars(customDataset)

    result.collect.length shouldEqual 30
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

  "Calculate Character skew" should "give low values for many different types" in {
    val localContext = sparkContext
    val prep = new DefaultRemovePreambleImpl
    val customDataset = localContext.read
      .option("sep", " ")
      .csv("./src/test/resources/test_space_preamble.csv")

    RemovePreambleHelper.calculateCharacterTypeSkew(customDataset) shouldEqual 0.25
  }
}