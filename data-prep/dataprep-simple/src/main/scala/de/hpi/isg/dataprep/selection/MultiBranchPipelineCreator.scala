package de.hpi.isg.dataprep.selection
import de.hpi.isg.dataprep.selection.MultiBranchPipelineCreator._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col


class MultiBranchPipelineCreator(dataFrame: DataFrame) {
  val dummyPreparators = List(new DummyPreparator, new DummyPreparator, new DummyPreparator)
  val root: Root = Root(dataFrame)

  // Assembles the pipeline.
  def createPipeline(): Unit = {
    val bestBranch = findBestBranch(root)
    println(getBranchScore(bestBranch))
    println
    getBranchAffectedCols(bestBranch).toList.foreach(println)
    println(getBranchAffectedCols(bestBranch).size)
  }

  // Returns the leaf node that represents the branch with the highest total score.
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

  // Transforms a candidate to a tree node to append it to the graph.
  def candidateToBranchHead(candidate: Candidate): Child = {
    val oldBranchHead = candidate.branchHead
    val preparator = candidate.preparator
    val colComb = candidate.affectedCols
    val prepScore = candidate.score
    val oldDF = oldBranchHead.dataFrame
    val newDf = preparator.impl.execute(oldDF)

    Child(oldBranchHead, preparator, colComb, newDf, prepScore)
  }

  // Returns a map from all preparators in a branch to all columns they affected.
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

  // Filters the k best candidates from all potential candidates to extend the tree.
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

  // Returns the candidates to extend the tree branch represented by the branchHead. The List
  // of preparators represents all possible preparation operators.
  // The preparator applicabilities are calculated for all valid column combinations for each preparator.
  def nextCandidates(branchHead: TreeNode, preparators: List[DummyPreparator]): List[Candidate]    = {
    val branchScore = getBranchScore(branchHead)
    val affectedCols = getBranchAffectedCols(branchHead)
    for {
      prep <- preparators
      colComb <- generateColumnCombinations(branchHead.dataFrame, affectedCols.getOrElse(prep, Set.empty))
      score = prep.calApplicability(colComb)
    } yield Candidate(branchHead, prep, colComb.columns.toSet, branchScore + score, score)
  }

  // This generates all the column combinations and allows to exclude certain columns.
  // This allows us to prevent a preparator from affecting a column twice.
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
