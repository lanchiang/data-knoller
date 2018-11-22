package de.hpi.isg.dataprep

import org.apache.spark.sql._
import org.scalatest._

trait TestData {
  val data1 = List(
    "this,is,valid,data",
    "-----",
    "this,is,invalid,data"
  )
}

class ConversionHelperTest extends FlatSpec {

  "Split file with Separator" should "return two data sets" in new TestData {
    val seq = Seq(
      "hallo",
      "----",
      "welt"
    )
    //val df = seq.toDF()

    //val resDf = ConversionHelper.splitFileBySeparator("-", seq)
    //assert(resDf._1.isInstanceOf[DataFrame])
    //assert(resDf._2.isInstanceOf[DataFrame])
  }
}