package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.ConversionHelper.countSubstring
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class ConversionHelperTest extends WordSpec with Matchers with SparkContextSetup {
  "My analytics" should {
    "calculate the right thing" in withSparkContext { (sparkContext) =>
      val data = Seq(1,2,3,4, 990)
      val rdd = sparkContext.parallelize(data)
      val total = rdd.reduce(_ + _)

      total shouldBe 1000
    }
  }

  "Split File" should {
    "Tuple the input" in withSparkContext{ (sparkContext) =>
      val data = Seq("hallo","----", "world")
      val rdd = sparkContext.parallelize(data)
      val sc = SparkSession.builder
        .master("local")
        .appName("Word Count")
        .getOrCreate()
      import sc.implicits._
      val testFrame = rdd.toDF
      testFrame.collectAsList.toArray shouldBe(List(Row("hallo"),Row("----"), Row("world")).toArray)
      val mappedRows = testFrame.collectAsList.toArray().map(x => (x, countSubstring(x.toString(), "-"))).toList
      mappedRows shouldBe(List((Row("hallo"), 0), (Row("----"), 4), (Row("world"), 0)))
    }

    "Find the Row with the most separators" in withSparkContext{ (sparkContext) =>

      val maximumSepRow = List((Row("hallo"), 0), (Row("----"), 4), (Row("world"), 0)).toArray
        .maxBy(item => item._2)
      maximumSepRow shouldBe((Row("----"), 4))
    }

    "return a Dataset" in withSparkContext{ (sparkContext) =>
      val data = Seq("hallo","----", "world")
      val rdd = sparkContext.parallelize(data)
      val sc = SparkSession.builder
        .master("local")
        .appName("Word Count")
        .getOrCreate()
      import sc.implicits._
      val testFrame = rdd.toDF
      val firstDataset = ConversionHelper.splitFileBySeparator("-", testFrame)
      firstDataset.collect shouldBe(List(Row("hallo")).toArray)
    }

    "works on Datasets with multiple rows" in withSparkContext { (sparkContext) =>
      val data = Seq(("hallo", "ballo"),("----", "------"),("world", "noerld"))
      val rdd = sparkContext.parallelize(data)
      val sc = SparkSession.builder
        .master("local")
        .appName("Word Count")
        .getOrCreate()
      import sc.implicits._
      val testFrame = rdd.toDF()
      val firstDataset = ConversionHelper.splitFileBySeparator("-", testFrame)
      firstDataset.collect shouldBe(List(Row("hallo","ballo")).toArray)
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
