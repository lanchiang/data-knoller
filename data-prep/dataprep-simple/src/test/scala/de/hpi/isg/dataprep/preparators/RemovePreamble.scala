package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.preparators.define.ChangeDateFormat
import de.hpi.isg.dataprep.preparators.implementation.{DateRegex, DefaultChangeDateFormatImpl}
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.sql.{Row, SparkSession}
import de.hpi.isg.dataprep.preparators.implementation.DefaultRemovePreambleImpl

import scala.collection.JavaConverters._

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

  "Dataset" should "be correctly read" in {
    val localContext = sparkContext
    import localContext.implicits._
    val customDataset = Seq(("1","2"), ("3","4"), ("postamble",""),("postamble",""),("postamble",""), ("","fake")).toDF
    customDataset.columns.length shouldEqual(2)
    val test = customDataset
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
    test.sortBy(a => a) shouldEqual(List(1,3))
  }

  "Remove Preamble" should "calculate calApplicability for multiple collumns correctly" in {
    val localContext = sparkContext
    import localContext.implicits._
    val customDataset = Seq(("1","2"), ("3","4"), ("postamble",""),("postamble",""),("postamble",""), ("","fake")).toDF
    customDataset.columns.length shouldEqual(2)
    val result = customDataset
        .map(row => row.mkString)
    .map(_.toString)
    .collect
    .reduce( _+ "|" + _)
    result shouldEqual("12|34|postamble|postamble|postamble|fake")
  }

  "Remove Preamble" should "find the correct first char in each line" in {
    val localContext = sparkContext
    import localContext.implicits._
    val customDataset = Seq(("1","2"), ("3","4"), ("postamble",""),("postamble",""),("postamble",""),("1", "3"), ("","fake")).toDF
    customDataset.columns.length shouldEqual(2)

    val test = customDataset
      .rdd
      .zipWithIndex()
      .map(e => (e._1.toString().charAt(1),List(e._2)))
      .reduceByKey(_.union(_))
      .flatMap(row => row._2.groupBy(k => k - row._2.indexOf(k)).toList.map(group => (row._1, group._2.size)))
      .filter(row => row._1.toString.matches("[^0-9]"))
    .map(_.toString)
    .collect
    .reduce( _+ "|" + _)
    test shouldEqual("(p,3)|(,,1)")
  }

  "Clustering" should "find correct clusters in dataset with separators" in {
    val localContext = sparkContext
    import localContext.implicits._
    val customDataset = Seq("1,2", "3,4", "postamble ", "postamble ","postamble ","1,3", "7,8,").toDF
    customDataset.columns.length shouldEqual(1)

    val testPreparator = new DefaultRemovePreambleImpl
    val prepResult = testPreparator.findPreambleByClustering(customDataset, ",")
    prepResult.collect() shouldEqual("(p,3)|(,,1)")
  }

}