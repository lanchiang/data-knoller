package de.hpi.isg.dataprep.selection


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class MultiBranchPipelineCreatorTest extends FlatSpecLike with Matchers with BeforeAndAfterAll {

  var preparators: List[DummyPreparator] = _
  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  var df: DataFrame = _

  override def beforeAll(): Unit = {
    val dummyPrep1 = new DummyPreparator("prep1")
    val dummyPrep2 = new DummyPreparator("prep2")
    val dummyPrep3 = new DummyPreparator("prep3")

    preparators = List(dummyPrep1, dummyPrep2, dummyPrep3)
    import spark.implicits._
    val testData = List((1,2,3,4))
    df = testData.toDF("col1", "col2", "col3", "col4")
  }

  "MultiBranchPipelineCreator" should "generate correct column combinations" in {
    val dummyPrep = new DummyPreparator("prep")

    val pipelineCreator = new MultiBranchPipelineCreator(df)
    val columnCombinations = pipelineCreator.generateColumnCombinations(df, Set("col1"))

    columnCombinations should have size 7
  }

  it should "find the affected columns for a branch" in {
    val root = MultiBranchPipelineCreator.Root(df)
    val innerNode1 = MultiBranchPipelineCreator.Child(root, preparators(0), Set("col1"), df, 0.8f)
    val innerNode2 = MultiBranchPipelineCreator.Child(innerNode1, preparators(1), Set("col4"), df, 0.8f)
    val leaf = MultiBranchPipelineCreator.Child(innerNode2, preparators(0), Set("col2", "col3"), df, 0.5f)

    val pipelineCreator = new MultiBranchPipelineCreator(df)
    pipelineCreator.getBranchAffectedCols(leaf) should be (Map(preparators(0)->Set("col1", "col2", "col3"),preparators(1)->Set("col4")))
  }

  it should "calculate the correct branch score" in {
    val root = MultiBranchPipelineCreator.Root(df)
    val innerNode = MultiBranchPipelineCreator.Child(root, preparators(0), Set.empty, df, 0.8f)
    val leaf = MultiBranchPipelineCreator.Child(innerNode, preparators(1), Set.empty, df, 0.5f)

    val pipelineCreator = new MultiBranchPipelineCreator(df)
    pipelineCreator.getBranchScore(leaf) should equal (1.3f)
  }

  it should "test" in {
    val pipelineCreator = new MultiBranchPipelineCreator(df.select("col1", "col2"))
    pipelineCreator.createPipeline()
  }
}
