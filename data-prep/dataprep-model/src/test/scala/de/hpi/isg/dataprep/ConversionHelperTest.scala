package de.hpi.isg.dataprep

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql._
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

class test extends FunSuite with DataFrameSuiteBase {
  test("simple Test"){
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input1 = sc.parallelize(List(1, 2, 3)).toDF
    assertDataFrameEquals(input1, input1) // equal

    val input2 = sc.parallelize(List(4, 5, 6)).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(input1, input2) // not equal
    }
  }
}

class ConversionHelperTest extends FunSuite with DataFrameSuiteBase {

  test("Dataframe is split by Separator if given"){
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = sc.parallelize(List("hallo", "----", "Welt")).toDF

    val splittedDataframes = ConversionHelper.splitFileBySeparator("-", input)

    assertDataFrameEquals(splittedDataframes.head, sc.parallelize(List("hallo")).toDF)
    assertDataFrameEquals(splittedDataframes.last, sc.parallelize(List("Welt")).toDF)
  }
}