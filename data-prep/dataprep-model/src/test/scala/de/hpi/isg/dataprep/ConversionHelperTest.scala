package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.ConversionHelper.countSubstring
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class ConversionHelperTest extends WordSpec with Matchers with SparkContextSetup {
//  "My analytics" should {
//    "calculate the right thing" in withSparkContext { (sparkContext) =>
//      val data = Seq(1, 2, 3, 4, 990)
//      val rdd = sparkContext.parallelize(data)
//      val total = rdd.reduce(_ + _)
//
//      total shouldBe 1000
//    }
//  }
//
//  "Split File" should {
//    "Tuple the input" in withSparkContext { (sparkContext) =>
//      val data = Seq("hallo", "----", "world")
//      val rdd = sparkContext.parallelize(data)
//      val sc = SparkSession.builder
//        .master("local")
//        .appName("Word Count")
//        .getOrCreate()
//      import sc.implicits._
//      val testFrame = rdd.toDF
//      testFrame.collectAsList.toArray shouldBe (List(Row("hallo"), Row("----"), Row("world")).toArray)
//      val mappedRows = testFrame.collectAsList.toArray().map(x => (x, countSubstring(x.toString(), "-"))).toList
//      mappedRows shouldBe (List((Row("hallo"), 0), (Row("----"), 4), (Row("world"), 0)))
//    }
//
//    "Find the Row with the most separators" in withSparkContext { (sparkContext) =>
//
//      val maximumSepRow = List((Row("hallo"), 0), (Row("----"), 4), (Row("world"), 0)).toArray
//        .maxBy(item => item._2)
//      maximumSepRow shouldBe ((Row("----"), 4))
//    }
//
//    "return a Dataset" in withSparkContext { (sparkContext) =>
//      val data = Seq("hallo", "----", "world")
//      val rdd = sparkContext.parallelize(data)
//      val sc = SparkSession.builder
//        .master("local")
//        .appName("Word Count")
//        .getOrCreate()
//      import sc.implicits._
//      val testFrame = rdd.toDF
//      val firstDataset = ConversionHelper.splitFileBySeparator("-", testFrame)
//      firstDataset.collect shouldBe (List(Row("hallo")).toArray)
//    }
//
//    "works on Datasets with multiple rows" in withSparkContext { (sparkContext) =>
//      val data = Seq(("hallo", "ballo"), ("----", "------"), ("world", "noerld"))
//      val rdd = sparkContext.parallelize(data)
//      val sc = SparkSession.builder
//        .master("local")
//        .appName("Word Count")
//        .getOrCreate()
//      import sc.implicits._
//      val testFrame = rdd.toDF()
//      val firstDataset = ConversionHelper.splitFileBySeparator("-", testFrame)
//      firstDataset.collect shouldBe (List(Row("hallo", "ballo")).toArray)
//    }
//  }
//
//  "Find Separator" should {
//    "find the most common non-alphanumeric character in the file" in withSparkContext { (sparkContext) =>
//      val data = Seq(("hallo", "ballo"), ("-----", "------"), ("world", "noerld"))
//      val rdd = sparkContext.parallelize(data)
//      val sc = SparkSession.builder
//        .master("local")
//        .appName("Word Count")
//        .getOrCreate()
//      import sc.implicits._
//      val testFrame = rdd.toDF()
//      val separator = ConversionHelper.findUnknownFileSeparator(testFrame)
//
//      separator._1 shouldBe ("-")
//    }
//  }
//
//  "Find empty values" should {
//    "find 3 emtpy values in one row one below the other" in withSparkContext { (sparkContext) =>
//      val data = Seq(("hallo", "ballo"), ("world", "noerld"), ("bla", ""), ("abc", ""), ("xyz", ""))
//      val rdd = sparkContext.parallelize(data)
//      val sc = SparkSession.builder
//        .master("local")
//        .appName("Word Count")
//        .getOrCreate()
//      import sc.implicits._
//      val testFrame = rdd.toDF()
//      val nullValue = ConversionHelper.splitFileByEmptyValues(testFrame)
//
//      nullValue.collect() shouldBe (List(Row("hallo", "ballo"), Row("world", "noerld")).toArray)
//    }
//  }
//
//  "Find new values" should {
//    "Find new values under 4 empty values in one row" in withSparkContext { (sparkContext) =>
//      val data = Seq(
//        ("world", ""),
//        ("bla", ""),
//        ("abc", ""),
//        ("xyz", "sss"),
//        ("gef", "fef"),
//        ("lfe", "das"))
//      val rdd = sparkContext.parallelize(data)
//      val sc = SparkSession.builder
//        .master("local")
//        .appName("Word Count")
//        .getOrCreate()
//      import sc.implicits._
//      val testFrame = rdd.toDF()
//      val nullValue = ConversionHelper.splitFileByNewValuesAfterEmpty(testFrame)
//
//      nullValue.collect() shouldBe (List(Row("xyz", "sss"), Row("gef", "fef"), Row("lfe", "das")).toArray)
//    }
//  }


  //"Split dataset by datatype" should {
  //"read csv files nicely" in withSparkContext{ (sparkContext) =>
  //  val sc = SparkSession.builder
  //    .master("local")
  //    .appName("Word Count")
  //    .getOrCreate()
  //
  //  val dialect = new DialectBuilder().hasHeader(true).inferSchema(true).url("./src/test/resources/multipleFilesWithDataTypes.csv").buildDialect
  //  val dataLoader = new FlatFileDataLoader(dialect)
  //  val dataContext = dataLoader.load
  //
  //  dataFrame shouldBe(1)
  //}

  // "identify all the different datatypes in one column" in withSparkContext{ (sparkContext) =>
  //   val data = Seq(("ID", "AGE"),(1, 10),(2, 30),("date", "time"), ("1.1.18", true), ("2.3.19", true))
  //   val rdd = sparkContext.parallelize(data)
  //   val sc = SparkSession.builder
  //     .master("local")
  //     .appName("Word Count")
  //     .getOrCreate()
  //   import sc.implicits._
  //   val testFrame = rdd.toDF()
  //   testFrame shouldBe(1)
  //   var typeMap = collection.mutable.Map[String, List[DataType]]()
  //   testFrame.collect.foreach( row => {
  //     val typeIterator = row.schema.iterator
  //     while(typeIterator.hasNext){
  //       val field = typeIterator.next
  //       if(typeMap.keySet.contains(field.name)){
  //         typeMap(field.name) = field.dataType :: typeMap(field.name)
  //       }else{
  //         typeMap = typeMap + (field.name -> List[DataType]())
  //       }
  //     }
  //   })
  //   typeMap.toArray shouldBe(Array(1))
  // }
  //
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
