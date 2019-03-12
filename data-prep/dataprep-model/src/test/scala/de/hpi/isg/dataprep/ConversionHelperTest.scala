package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.ConversionHelper.countSubstring
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class ConversionHelperTest extends WordSpec with Matchers with SparkContextSetup {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  "My analytics" should {
    "calculate the right thing" in withSparkContext { (sparkContext) =>
      val data = Seq(1, 2, 3, 4, 990)
      val rdd = sparkContext.parallelize(data)
      val total = rdd.reduce(_ + _)

      total shouldBe 1000
    }
  }

  "Split File" should {
    "Tuple the input" in withSparkContext { (sparkContext) =>
      val data = Seq("hallo", "----", "world")
      val rdd = sparkContext.parallelize(data)
      val sc = SparkSession.builder
        .master("local")
        .appName("Word Count")
        .getOrCreate()
      import sc.implicits._
      val testFrame = rdd.toDF
      testFrame.collectAsList.toArray shouldBe (List(Row("hallo"), Row("----"), Row("world")).toArray)
      val mappedRows = testFrame.collectAsList.toArray().map(x => (x, countSubstring(x.toString(), "-"))).toList
      mappedRows shouldBe (List((Row("hallo"), 0), (Row("----"), 4), (Row("world"), 0)))
    }

    "Find the Row with the most separators" in withSparkContext { (sparkContext) =>

      val maximumSepRow = List((Row("hallo"), 0), (Row("----"), 4), (Row("world"), 0)).toArray
        .maxBy(item => item._2)
      maximumSepRow shouldBe ((Row("----"), 4))
    }

    "return a Dataset" in withSparkContext { (sparkContext) =>
      val data = Seq("hallo", "----", "world")
      val rdd = sparkContext.parallelize(data)
      val sc = SparkSession.builder
        .master("local")
        .appName("Word Count")
        .getOrCreate()
      import sc.implicits._
      val testFrame = rdd.toDF
      val firstDataset = ConversionHelper.splitFileBySeparator("-", testFrame)
      firstDataset.collect shouldBe (List(Row("hallo")).toArray)
      // but there is no unit test to check the content of the saved file.
    }

    "works on Datasets with multiple rows" in withSparkContext { (sparkContext) =>
      val data = Seq(("hallo", "ballo"), ("----", "------"), ("world", "noerld"))
      val rdd = sparkContext.parallelize(data)
      val sc = SparkSession.builder
        .master("local")
        .appName("Word Count")
        .getOrCreate()
      import sc.implicits._
      val testFrame = rdd.toDF()
      val firstDataset = ConversionHelper.splitFileBySeparator("-", testFrame)
      firstDataset.collect shouldBe (List(Row("hallo", "ballo")).toArray)
    }
  }

  "Find Separator" should {
    "find the most common non-alphanumeric character in the file" in withSparkContext { (sparkContext) =>
      val data = Seq(("hallo", "ballo"), ("-----", "------"), ("world", "noerld"))
      val rdd = sparkContext.parallelize(data)
      val sc = SparkSession.builder
        .master("local")
        .appName("Word Count")
        .getOrCreate()
      import sc.implicits._
      val testFrame = rdd.toDF()
      val separator = ConversionHelper.findUnknownFileSeparator(testFrame)
      separator._1 shouldBe ("-")
    }
  }

  "Find empty values" should {
    "find 3 emtpy values in one row one below the other" in withSparkContext { (sparkContext) =>
      val data = Seq(("hallo", "ballo"), ("world", "noerld"), ("bla", ""), ("abc", ""), ("xyz", ""))
      val rdd = sparkContext.parallelize(data)
      val sc = SparkSession.builder
        .master("local")
        .appName("Word Count")
        .getOrCreate()
      import sc.implicits._
      val testFrame = rdd.toDF()
      val nullValue = ConversionHelper.splitFileByEmptyValues(testFrame)
      nullValue.collect() shouldBe (List(Row("hallo", "ballo"), Row("world", "noerld")).toArray)
    }
  }

  "Find new values" should {
    "Find new values under 4 empty values in one row" in withSparkContext { (sparkContext) =>
      val data = Seq(
        ("world", ""),
        ("bla", ""),
        ("abc", ""),
        ("xyz", "sss"),
        ("gef", "fef"),
        ("lfe", "das"))
      val rdd = sparkContext.parallelize(data)
      val sc = SparkSession.builder
        .master("local")
        .appName("Word Count")
        .getOrCreate()
      import sc.implicits._
      val testFrame = rdd.toDF()
      val nullValue = ConversionHelper.splitFileByNewValuesAfterEmpty(testFrame)
      nullValue.collect() shouldBe (List(Row("xyz", "sss"), Row("gef", "fef"), Row("lfe", "das")).toArray)
    }
  }
}

trait SparkContextSetup {
  def withSparkContext(testMethod: (SparkContext) => Any) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark test")
    val sparkContext = new SparkContext(conf)
    try {
      testMethod(sparkContext)
    }
    finally sparkContext.stop()
  }
}
