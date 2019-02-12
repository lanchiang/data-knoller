package de.hpi.isg.dataprep.selection
import de.hpi.isg.dataprep.selection.MultiBranchPipelineCreator._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col


class MultiBranchPipelineCreator(dataFrame: DataFrame) {
  val dummyPreparators = List(new DummyPreparator, new DummyPreparator, new DummyPreparator)
  val root: Root = Root(dataFrame)

  def createPipeline(): Unit = {
    val bestBranch = findBestBranch(root)
    println(getBranchScore(bestBranch))
    println
    getBranchAffectedCols(bestBranch).toList.foreach(println)
    println(getBranchAffectedCols(bestBranch).size)
  }

  def findBestBranch(root: Root): Child = {
    def rec(branchHeads: List[TreeNode], maxIterations: Int): List[TreeNode] = {
      if (maxIterations == 0) return branchHeads

      val candidates = branchHeads
        .flatMap(branchHead => {
          nextCandidates(branchHead, dummyPreparators)
        })
        .filter(_.score > PREPARATOR_SCORE_THRESHOLD)

      if (candidates.size < MAX_BRANCHES) return branchHeads

      val bestCandidates = kBestCandidates(MAX_BRANCHES, candidates)
      rec(bestCandidates.map(candidateToBranchHead), maxIterations-1)
    }
    val branchHeads = rec(List(root), MAX_ITERATIONS)
    branchHeads match {
      case childs: List[Child] => childs.maxBy(getBranchScore(_))
      case root: List[Root] => throw new RuntimeException("No preparators found.")
    }
  }

  def candidateToBranchHead(candidate: Candidate): Child = {
    val oldBranchHead = candidate.branchHead
    val preparator = candidate.preparator
    val colComb = candidate.affectedCols
    val prepScore = candidate.score
    val oldDF = oldBranchHead.dataFrame
    val newDf = preparator.impl.execute(oldDF)

    Child(oldBranchHead, preparator, colComb, newDf, prepScore)
  }

  def getBranchAffectedCols(node: TreeNode): Map[DummyPreparator, Set[String]] = node match {
    case Root(_) => Map.empty[DummyPreparator, Set[String]]
    case Child(parent, preparator, affectedCols, _, _) => {
      val branchAffectedCols = getBranchAffectedCols(parent)
      val preparatorAffectedCols = branchAffectedCols
        .getOrElse(preparator, Set.empty)
        .union(affectedCols)

      branchAffectedCols + (preparator -> preparatorAffectedCols)
    }
  }

  def kBestCandidates(k: Int, candidates: List[Candidate]): List[Candidate] = {
    if(k == 0) Nil
    else {
      val bestCandidate = candidates.maxBy(_.totalScore)
      bestCandidate +: kBestCandidates(k-1, candidates.filterNot(_ == bestCandidate))
    }
  }

  // calculates score of the branch up to the (and including) the given node
  def getBranchScore(node: TreeNode): Score = node match {
      case Root(_) => 0f
      case Child(parent, _, _, _, score) => score + getBranchScore(parent)
  }

  def nextCandidates(branchHead: TreeNode, preparators: List[DummyPreparator]): List[Candidate]    = {
    val branchScore = getBranchScore(branchHead)
    val affectedCols = getBranchAffectedCols(branchHead)
    for {
      prep <- preparators
      colComb <- generateColumnCombinations(branchHead.dataFrame, affectedCols.getOrElse(prep, Set.empty))
      score = prep.calApplicability(colComb)
    } yield Candidate(branchHead, prep, colComb.columns.toSet, branchScore + score, score)
  }

  def generateColumnCombinations(df: DataFrame, excludeCols: Set[String]): List[DataFrame] = {
    (df.columns.toSet -- excludeCols)
      .subsets()
      .filterNot(_.isEmpty)
      .map(_.map(col))
      .map(cols => df.select(cols.toSeq:_*))
      .toList
  }

}

object MultiBranchPipelineCreator {
  private val MAX_ITERATIONS = 100
  private val MAX_BRANCHES = 2
  private val PREPARATOR_SCORE_THRESHOLD = 0.5
  type Score = Float

  sealed trait TreeNode {
    def dataFrame: DataFrame
  }

  case class Root(dataFrame: DataFrame) extends TreeNode

  case class Child(parent: TreeNode,
                   preparator: DummyPreparator,
                   affectedCols: Set[String],
                   dataFrame: DataFrame,
                   score: Score
                 ) extends TreeNode

  case class Candidate(branchHead: TreeNode, preparator: DummyPreparator, affectedCols: Set[String], totalScore: Score, score: Score)
}
