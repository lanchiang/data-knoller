package de.hpi.isg.dataprep.selection


import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class MultiBranchPipelineCreatorTest extends FlatSpecLike with Matchers with BeforeAndAfterAll {

  var preparators: List[DummyPreparator] = _


  override def beforeAll(): Unit = {
    val dummyPrep1 = new DummyPreparator("prep1")
    val dummyPrep2 = new DummyPreparator("prep2")
    val dummyPrep3 = new DummyPreparator("prep3")

    preparators = List(dummyPrep1, dummyPrep2, dummyPrep3)
  }

  "MultiBranchPipelineCreator" should "generate correct column combinations" in {
    val testData = List((1,2,3,4))
    val spark = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._
    val df = testData.toDF("col1", "col2", "col3", "col4")

    val dummyPrep = new DummyPreparator("prep")

    val pipelineCreator = new MultiBranchPipelineCreator(df)
    val columnCombinations = pipelineCreator.generateColumnCombinations(dummyPrep, df, Set("col1"))

    columnCombinations should have size 8
  }

  it should "find the affected columns for a branch" in {


  }

  it should "calculate the correct branch score" in {

  }

}
