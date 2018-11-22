package de.hpi.isg.dataprep

import org.apache.spark.sql.{DataFrame, Row, _}
import org.scalatest._
import org.scalamock.scalatest.MockFactory

/*
trait TestData {
  val data1 = List(
    "this,is,valid,data",
    "-----",
    "this,is,invalid,data"
  )
}
*/

class ConversionHelperTest extends FlatSpec with MockFactory {

  def splitFileBySeparator: Unit = {
    val testFile = Seq(
      "hallo",
      "----",
      "welt"
    )



  }

  /*
  "Split file with Separator" should "return two data sets" in new TestData {
    val testFile = Seq(
      "hallo",
      "----",
      "welt"
    )

    val expectedDatasetOne = Seq (
      "hallo"
    )
    val expectedDatasetTwo = Seq (
      "welt"
    )

*/
    //val df = testFile.toDF()

    //val resDf = ConversionHelper.splitFileBySeparator("-", testFile)
    //assert(resDf._1.isInstanceOf[DataFrame])
    //assert(resDf._2.isInstanceOf[DataFrame])
  }
}