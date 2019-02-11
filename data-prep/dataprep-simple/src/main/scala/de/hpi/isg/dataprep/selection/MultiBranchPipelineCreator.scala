package de.hpi.isg.dataprep.selection
import de.hpi.isg.dataprep.components.Pipeline
import de.hpi.isg.dataprep.context.DataContext
import org.apache.spark.sql.DataFrame

class MultiBranchPipelineCreator(dataContext: DataContext) extends PipelineCreator(dataContext) with PreparatorLoader {
  //override val path: String = "de.hpi.isg.dataprep.preparators"
  val dummyPreparators = List(new DummyPreparator, new DummyPreparator, new DummyPreparator)

  private val MAX_ITERATIONS = 100

  private case class Node(
                            parent: Option[Node],
                            childs: Option[List[Node]],
                            preparator: Option[DummyPreparator],
                            affectedCols: Map[DummyPreparator, Set[String]],
                            dataset: DataFrame,
                            score: Double
                         )

  val root = Node(
    None,
    None,
    None,
    Map.empty[DummyPreparator, Set[String]],
    dataContext.getDataFrame,
    0
  )

  override def createPipeline(): Pipeline = {
  }

  def generateColumnCombinations(preparator: DummyPreparator, df: DataFrame, affectedCols: Set[String]): Set[DataFrame] = {
    // generate all valid column combinations for the preparator
    ???
  }

}
