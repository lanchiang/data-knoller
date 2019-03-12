package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.ConversionHelper
import de.hpi.isg.dataprep.ConversionHelper.countSubstring
import org.apache.spark.sql.{Row, SparkSession}


class ConversionHelperSplitFileTest extends PreparatorScalaTest {
  var sparkContext:SparkSession = _

  override var resourcePath = "/pokemon.csv"

  override def beforeAll: Unit = {
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]")
    sparkContext = sparkBuilder.getOrCreate()
    super.beforeAll()
  }

  "Split File" should "Tuple the input" in {
    val localContext = sparkContext
    import localContext.implicits._
    val data = Seq("hallo", "----", "world").toDF

    data.collectAsList.toArray shouldBe Array(Row("hallo"), Row("----"), Row("world"))
    val mappedRows = data.collectAsList.toArray().map(x => (x, countSubstring(x.toString(), "-"))).toList
    mappedRows shouldBe List((Row("hallo"), 0), (Row("----"), 4), (Row("world"), 0))
  }

  "Split File" should "Find the Row with the most separators" in {

    val maximumSepRow = List((Row("hallo"), 0), (Row("----"), 4), (Row("world"), 0)).toArray
      .maxBy(item => item._2)
    maximumSepRow shouldBe ((Row("----"), 4))
  }

  "Split File" should "return a Dataset" in {
    val localContext = sparkContext

    import localContext.implicits._
    val testFrame = Seq("hallo", "----", "world").toDF
    val firstDataset = ConversionHelper.splitFileBySeparator("-", testFrame)
    firstDataset.collect shouldBe Array(Row("hallo"))
  }

  "Split File" should "work on Datasets with multiple rows" in {
    val localContext = sparkContext
    import localContext.implicits._
    val data = Seq(("hallo", "ballo"), ("----", "------"), ("world", "noerld")).toDF

    val firstDataset = ConversionHelper.splitFileBySeparator("-", data)
    firstDataset.collect shouldBe Array(Row("hallo", "ballo"))
  }

  "Find Separator" should "find the most common non-alphanumeric character in the file" in {
    val localContext = sparkContext
    import localContext.implicits._
    val data = Seq(("hallo", "ballo"), ("-----", "------"), ("world", "noerld")).toDF

    val separator = ConversionHelper.findUnknownFileSeparator(data)
    separator._1 shouldBe ("-")

  }

  "Find empty values" should "find 3 emtpy values in one row one below the other" in  {
    val localContext = sparkContext
    import localContext.implicits._
    val data = Seq(("hallo", "ballo"), ("world", "noerld"), ("bla", ""), ("abc", ""), ("xyz", "")).toDF

    val nullValue = ConversionHelper.splitFileByEmptyValues(data)
    nullValue.collect() shouldBe Array(Row("hallo", "ballo"), Row("world", "noerld"))
  }

  "Find new values" should "Find new values under 4 empty values in one row" in  {
    val localContext = sparkContext
    import localContext.implicits._
    val data = Seq(
      ("world", ""),
      ("bla", ""),
      ("abc", ""),
      ("xyz", "sss"),
      ("gef", "fef"),
      ("lfe", "das")).toDF

    val nullValue = ConversionHelper.splitFileByNewValuesAfterEmpty(data)
    nullValue.collect() shouldBe Array(Row("xyz", "sss"), Row("gef", "fef"), Row("lfe", "das"))
  }
}