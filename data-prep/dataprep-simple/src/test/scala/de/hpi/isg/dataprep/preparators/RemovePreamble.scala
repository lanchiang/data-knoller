package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.preparators.define.ChangeDateFormat
import de.hpi.isg.dataprep.preparators.implementation.{DateRegex, DefaultChangeDateFormatImpl}
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class RemovePreamble extends PreparatorScalaTest {

  "Dataset" should "be correctly read" in {
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()
    import spark.implicits._
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
      //.map(_.toString)
      //.collect
      //.reduce( _+ "|" + _)
    test shouldEqual("ol")
  }

  "RemovePreable" should "calculate calApplicability for multiple collumns correctly" in {
    1 shouldEqual(1)
  }

  "RemovePreable" should "find the correct first char in each line" in {
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()
    import spark.implicits._
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
    test shouldEqual("ol")
  }

}