package de.hpi.isg.dataprep.selection


import de.hpi.isg.dataprep.context.DataContext
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.DummyPreparator
import de.hpi.isg.dataprep.preparators.define.{ChangeDataType, DeleteProperty}

import scala.collection.JavaConversions._

class MultiBranchPipelineCreatorTest extends DataLoadingConfig {

  val columns = "id,identifier,species_id,height,weight,base_experience,order,is_default,date,stemlemma,stemlemma2,stemlemma_wrong".split(",").toList

  "MultiBranchPipelineCreator" should "generate correct column combinations" in {

    val pipelineCreator = new TestMultiBranchCreator(dataContext)
    val columnCombinations = pipelineCreator.generateColumnCombinations(dataContext.getDataFrame, columns.drop(3).toSet)

    columnCombinations should have size 7
  }

  it should "find the affected columns for a branch" in {
    val dummy1 = new DeleteProperty()
    val dummy2 = new ChangeDataType()

    val root = MultiBranchPipelineCreator.Root(dataContext.getDataFrame)
    val innerNode1 = MultiBranchPipelineCreator.Child(root, dummy1, Set("weight"), dataContext.getDataFrame, 0.8f)
    val innerNode2 = MultiBranchPipelineCreator.Child(innerNode1, dummy2, Set("height"), dataContext.getDataFrame, 0.8f)
    val leaf = MultiBranchPipelineCreator.Child(innerNode2, dummy1, Set("stemlemma", "height"), dataContext.getDataFrame, 0.5f)

    val pipelineCreator = new MultiBranchPipelineCreator(dataContext)
    pipelineCreator.getBranchAffectedCols(leaf) should be(Map(dummy1.getClass -> Set("weight", "height", "stemlemma"), dummy2.getClass -> Set("height")))
  }

  it should "calculate the correct branch score" in {
    val dummy1 = new DummyPreparator("dummy1")
    val dummy2 = new DummyPreparator("dummy2")

    val root = MultiBranchPipelineCreator.Root(dataContext.getDataFrame)
    val innerNode = MultiBranchPipelineCreator.Child(root, dummy1, Set.empty, dataContext.getDataFrame, 0.8f)
    val leaf = MultiBranchPipelineCreator.Child(innerNode, dummy2, Set.empty, dataContext.getDataFrame, 0.5f)

    val pipelineCreator = new TestMultiBranchCreator(dataContext)
    pipelineCreator.getBranchScore(leaf) should equal(1.3f)
  }

  it should "select trivial preparator" in {
    class TestMultiBranchCreator(dataContext: DataContext) extends MultiBranchPipelineCreator(dataContext) {
      override val preparators: List[Class[_ <: AbstractPreparator]] = List(classOf[DeleteProperty])

      val pipelineCreator = new TestMultiBranchCreator(dataContext)
      val pipe = pipelineCreator.createPipeline()

      val expected = new DeleteProperty("date")

      pipe.getPreparations.map(_.getAbstractPreparator) should contain (expected)

    }

  }
}

class TestMultiBranchCreator(dataContext: DataContext) extends MultiBranchPipelineCreator(dataContext) {
  override val preparators: List[Class[_ <: AbstractPreparator]] = List(classOf[DummyPreparator])
}


