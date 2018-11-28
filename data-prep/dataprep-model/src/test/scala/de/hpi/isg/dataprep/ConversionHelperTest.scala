package de.hpi.isg.dataprep

import org.apache.spark.sql.{SQLContext, SparkSession}
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
      1 shouldBe(1)
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
