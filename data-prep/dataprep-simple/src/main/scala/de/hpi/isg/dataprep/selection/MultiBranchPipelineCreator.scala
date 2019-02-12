package de.hpi.isg.dataprep.selection
import de.hpi.isg.dataprep.components.Pipeline
import de.hpi.isg.dataprep.selection.MultiBranchPipelineCreator._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col

import scala.collection.mutable.MutableList


class MultiBranchPipelineCreator(dataFrame: DataFrame) {
  val dummyPreparators = List(new DummyPreparator, new DummyPreparator, new DummyPreparator)

  val root = Node(
    None,
    MutableList.empty,
    None,
    Set.empty,
    dataFrame,
    0
  )



  def createPipeline(): Pipeline = {
    val leafs = List(root)

    val candidates = leafs
      .flatMap(leaf => {
        nextCandidates(leaf, dummyPreparators)
      })

    val bestCandidates = kBestCandidates(MAX_BRANCHES, candidates)
    ???


  }


  def getBranchAffectedCols(node: Node): Map[DummyPreparator, Set[String]] = node match {
    case Node(None, _, _, _, _, _) => Map.empty[DummyPreparator, Set[String]]
    case Node(Some(parent), _, Some(preparator), affectedCols, _, _) => {
      val branchAffectedCols = getBranchAffectedCols(parent)
      val preparatorAffectedCols = branchAffectedCols
        .getOrElse(preparator, Set.empty)
        .union(affectedCols)

      branchAffectedCols + (preparator -> preparatorAffectedCols)
    }
  }

  def kBestCandidates(k: Int, candidates: List[Candidate]): List[Candidate] = {
    if(k == 0) Nil
    else candidates.maxBy(c => c.totalScore) +: kBestCandidates(k-1, candidates)
  }

  // calculates score of the branch up to the (and including) the given node
  def getBranchScore(node: Node): Score = node match {
      case Node(None, _, _, _, _, 0f) => 0f
      case Node(Some(parent), _, _, _, _, score) => score + getBranchScore(parent)
  }

  def nextCandidates(leaf: Node, preparators: List[DummyPreparator]): List[Candidate]    = {
    val leafBranchScore = getBranchScore(leaf)
    preparators
      .map(p => Candidate(leaf, p, leafBranchScore + p.calApplicability(leaf.dataFrame)))
  }

  def generateColumnCombinations(preparator: DummyPreparator, df: DataFrame, affectedCols: Set[String]): List[DataFrame] = {
    (df.columns.toSet -- affectedCols)
      .subsets()
      .map(_.map(col))
      .map(cols => df.select(cols.toSeq:_*))
      .toList
  }

}

object MultiBranchPipelineCreator {
  private val MAX_ITERATIONS = 100
  private val MAX_BRANCHES = 2
  type Score = Float

  case class Node(
                   parent: Option[Node],
                   childs: MutableList[Node],
                   preparator: Option[DummyPreparator],
                   affectedCols: Set[String],
                   dataFrame: DataFrame,
                   score: Score
                 )

  case class Candidate(leaf: Node, preparator: DummyPreparator, totalScore: Score)
}
