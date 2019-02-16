package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.components.{Pipeline, Preparation}
import de.hpi.isg.dataprep.context.DataContext
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.selection.MultiBranchPipelineCreator._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


class MultiBranchPipelineCreator(dataContext: DataContext) extends PipelineCreator(dataContext) {
  val root: Root = Root(dataContext.getDataFrame)

  // Assembles the pipeline.
  def createPipeline(): Pipeline = {
    val bestBranch = findBestBranch(root)
    val newPipe = new Pipeline(bestBranch.dataFrame)
    addToPipe(bestBranch).foreach(newPipe.addPreparation)
    newPipe
  }

  def addToPipe(treeNode: TreeNode): List[Preparation] = treeNode match {
    case Root(_) => Nil
    case Child(parent, preparator, _, _, _) => addToPipe(parent) :+ new Preparation(preparator)
  }


  // Returns the leaf node that represents the branch with the highest total score.
  def findBestBranch(root: Root): Child = {
    def rec(branchHeads: List[TreeNode], maxIterations: Int): List[TreeNode] = {
      if (maxIterations == 0) return branchHeads

      val candidates = branchHeads
        .flatMap(branchHead => {
          nextCandidates(branchHead, preparators)
        })
        .filter(_.score > PREPARATOR_SCORE_THRESHOLD)

      if (candidates.size < MAX_BRANCHES) return branchHeads

      // Below order ensures that we only have to check k branches/candidates for equivalence.
      //      val bestCandidates = kBestCandidates(MAX_BRANCHES, candidates)
      //      val distinctCandidates = pruneEquivalentBranches(bestCandidates)

      val bestCandidates = pruneEquivalentBranches(candidates)
      val distinctCandidates = kBestCandidates(MAX_BRANCHES, bestCandidates)

      rec(distinctCandidates.map(candidateToBranchHead), maxIterations - 1)
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
    val prepScore = candidate.score
    val oldDf = oldBranchHead.dataFrame
    val newDf = preparator.execute(oldDf).newDataFrame
    // prevents operators like split attribute to recursively split columns
    val colComb = candidate.affectedCols.union(newDf.columns.toSet -- oldDf.columns.toSet)

    Child(oldBranchHead, preparator, colComb, newDf, prepScore)
  }

  // Returns a map from all preparators in a branch to all columns they affected.
  def getBranchAffectedCols(node: TreeNode): Map[Class[_ <: AbstractPreparator], Set[String]] = node match {
    case Root(_) => Map.empty[Class[_ <: AbstractPreparator], Set[String]]
    case Child(parent, preparator, affectedCols, _, _) => {
      val branchAffectedCols = getBranchAffectedCols(parent)
      val preparatorAffectedCols = branchAffectedCols
        .getOrElse(preparator.getClass, Set.empty)
        .union(affectedCols)

      branchAffectedCols + (preparator.getClass -> preparatorAffectedCols)
    }
  }

  def pruneEquivalentBranches(candidates: List[Candidate]): List[Candidate] = {
    def getBranchStructure(branchHead: TreeNode): List[(String, Set[String])] = branchHead match {
      case Root(_) => Nil
      case Child(parent, preparator, affectedCols, _, _) => (preparator.getClass.getSimpleName, affectedCols) :: getBranchStructure(parent)
    }

    def reduceBranchStructure(structure: List[(String, Set[String])]): List[Set[(String, Set[String])]] = {
      structure.foldLeft(List(Set.empty[(String, Set[String])])) { case (reducedStruct, (prep, cols)) =>
        val isIndependent = reducedStruct.head
          .exists { case (prevPrep, prevCols) => prevCols.intersect(cols).nonEmpty }

        if (isIndependent) (reducedStruct.head + ((prep, cols))) :: reducedStruct.tail
        else Set((prep, cols)) :: reducedStruct
      }
    }

    val distinctCandidates = candidates
      .map(c => (c, (c.preparator.getClass.getSimpleName, c.affectedCols) :: getBranchStructure(c.branchHead)))
      .map { case (candidate, unreducedBranch) => (candidate, reduceBranchStructure(unreducedBranch)) }
      .groupBy { case (candidate, reducedBranch) => reducedBranch } // Group equivalent candidates
      .map { case (key, iterator) => iterator.head._1 } // Map to unique candidate
      .toList

    distinctCandidates
  }

  // This dummy functions just show how schema and metadata similarity could be applied to modify the selected preparators
  def calculateSchemaSimilarity = 1

  def calculateMetaDataSimilarity = 1


  // Filters the k best candidates from all potential candidates to extend the tree.
  def kBestCandidates(k: Int, candidates: List[Candidate]): List[Candidate] = {
    if (k == 0) Nil
    else if (candidates.size <= k) candidates
    else {

      val bestCandidate = candidates.maxBy(c => {
        c.totalScore * calculateSchemaSimilarity * calculateMetaDataSimilarity
      })
      bestCandidate +: kBestCandidates(k - 1, candidates.filterNot(_ == bestCandidate))
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
  def nextCandidates(branchHead: TreeNode, preparators: List[Class[_ <: AbstractPreparator]]): List[Candidate] = {
    val branchScore = getBranchScore(branchHead)
    val affectedCols = getBranchAffectedCols(branchHead)
    for {
      classPrep <- preparators
      colComb <- generateColumnCombinations(branchHead.dataFrame, affectedCols.getOrElse(classPrep, Set.empty))
      prep = classPrep.newInstance()
      score = prep.calApplicability(dataContext.getSchemaMapping, colComb, dataContext.getTargetMetadata)
    } yield Candidate(branchHead, prep, colComb.columns.toSet, branchScore + score, score)
  }

  // This generates all the column combinations and allows to exclude certain columns.
  // This allows us to prevent a preparator from affecting a column twice.
  def generateColumnCombinations(df: DataFrame, excludeCols: Set[String]): List[DataFrame] = {
    (df.columns.toSet -- excludeCols)
      .subsets()
      .filterNot(_.isEmpty)
      .map(_.map(col))
      .map(cols => df.select(cols.toSeq: _*))
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
                   preparator: AbstractPreparator,
                   affectedCols: Set[String],
                   dataFrame: DataFrame,
                   score: Score
                  ) extends TreeNode

  case class Candidate(branchHead: TreeNode, preparator: AbstractPreparator, affectedCols: Set[String], totalScore: Score, score: Score)

}
