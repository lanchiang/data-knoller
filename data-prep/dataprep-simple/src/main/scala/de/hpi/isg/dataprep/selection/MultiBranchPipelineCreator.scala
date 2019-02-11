package de.hpi.isg.dataprep.selection
import de.hpi.isg.dataprep.components.Pipeline
import de.hpi.isg.dataprep.context.DataContext
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable.MutableList


class MultiBranchPipelineCreator(dataContext: DataContext) extends PipelineCreator(dataContext) with PreparatorLoader {
  override val path: String = "de.hpi.isg.dataprep.preparators"
  val dummyPreparators = List(new DummyPreparator, new DummyPreparator, new DummyPreparator)

  private val MAX_ITERATIONS = 100
  private val MAX_BRANCHES = 2
  type Score = Float

  case class Node(
                           parent: Option[Node],
                           childs: MutableList[Node],
                           preparator: Option[DummyPreparator],
                           affectedCols: Map[DummyPreparator, Set[String]],
                           dataFrame: DataFrame,
                           score: Score
                         )

  case class Candidate(leaf: Node, preparator: DummyPreparator, totalScore: Score)

  val root = Node(
    None,
    MutableList.empty,
    None,
    Map.empty[DummyPreparator, Set[String]],
    dataContext.getDataFrame,
    0
  )



  override def createPipeline(): Pipeline = {
    val leafs = List(root)

    val candidates = leafs
      .flatMap(leaf => {
        nextCandidates(leaf, dummyPreparators)
      })

    val bestCandidates = kBestCandidates(MAX_BRANCHES, candidates)



  ???
  }

  def kBestCandidates(k: Int, candidates: List[Candidate]): List[Candidate] = {
    if(k == 0) Nil
    else candidates.maxBy(c => c.totalScore) +: kBestCandidates(k-1, candidates)
  }

  // calculates score of the branch up to the (and including) the given node
  def getBranchScore(node: Node): Score = node match {
      case Node(None, _, _, _, _, _) => 0
      case Node(Some(parent), _, _, _, _, _) => getBranchScore(parent)
  }



  def nextCandidates(leaf: Node, preparators: List[DummyPreparator]): List[Candidate]    = {
    val leafBranchScore = getBranchScore(leaf)
    preparators
      .map(p => Candidate(leaf, p, leafBranchScore + p.calApplicability(leaf.dataFrame)))
      .toSet
  }

  def generateColumnCombinations(preparator: DummyPreparator, df: DataFrame, affectedCols: Set[String]): Set[DataFrame] = {
    (df.columns.toSet -- affectedCols)
      .subsets()
      .map(df.select(_:_*))
      .toSet
  }

}
