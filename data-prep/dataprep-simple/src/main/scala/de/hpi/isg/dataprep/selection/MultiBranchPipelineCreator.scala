package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.components.{Pipeline, Preparation}
import de.hpi.isg.dataprep.context.DataContext
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.selection.MultiBranchPipelineCreator._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
  * The MultiBranchPipelineCreator builds a tree of possible Preparation chains and evaluates multiple branches at the same time.
  * Finally, it returns the best chain of preparations as a pipeline.
  *
  * @param dataContext describes the input dataframe
  */
class MultiBranchPipelineCreator(dataContext: DataContext) extends PipelineCreator(dataContext) {
  /**
    * The root node of the tree.
    */
  val root: Root = Root(dataContext.getDataFrame)

  /**
    * Assembles a pipeline.
    *
    * @return finished Pipeline
    */
  override def createPipeline(): Pipeline = {
    val bestBranch = findBestBranch(root)
    val newPipe = new Pipeline(bestBranch.dataFrame)
    addToPipe(bestBranch).foreach(newPipe.addPreparation)
    newPipe
  }

  /**
    * Helper method to assemble a pipeline.
    *
    * @param treeNode branch head that identifies the best branch that will be transformed to a pipeline.
    * @return list of preparations in the best tree branch.
    */
  def addToPipe(treeNode: TreeNode): List[Preparation] = treeNode match {
    case Root(_) => Nil
    case Child(parent, preparator, _, _, _) => addToPipe(parent) :+ new Preparation(preparator)
  }


  /**
    * Given the root, a tree is iteratively built. The tree is pruned and stop conditions are evaluated in between.
    * Returns the leaf node that represents the branch with the highest total score.
    *
    * @param root root node of the tree
    * @return leaf with the highest total branch score
    */
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
      val bestCandidates = kBestCandidates(MAX_BRANCHES, candidates)
      val distinctCandidates = pruneEquivalentBranches(bestCandidates)

      rec(distinctCandidates.map(candidateToBranchHead), maxIterations - 1)
    }

    val branchHeads = rec(List(root), MAX_ITERATIONS)
    branchHeads match {
      case childs: List[Child] => childs.maxBy(getBranchScore(_))
      case root: List[Root] => throw new RuntimeException("No preparators found.")
    }
  }

  /**
    * Transforms a candidate to a tree node and appends it to the tree. Executes the candidates preparator to generate the
    * new intermediate dataset.
    *
    * @param candidate candidate that should be evaluated.
    * @return child node connected with the tree
    */
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

  /**
    * Returns a map from all preparator classes in a branch to all columns they affected.
    * This allows us to enforce that a preparator can only affect a column once and stop the tree
    * growing process accordingly.
    *
    * @param node node that represents a tree branch
    * @return map from preparator classes to their affected column set
    */
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

  /**
    * We pass a list of candidates and candidates that refer to equivalent branches get thrown away. An equivalent branch
    * is a branch that leads to the same intermediate dataset despite having a different operator order.
    * Preparators that affect different columns don't interfere with each other and can be applied in any order.
    *
    * @param candidates candidates to be pruned.
    * @return pruned candidates
    */
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

  /**
    * This dummy functions just shows how schema and metadata similarity could be applied to modify the selected preparators.
    * We can weigh the scores with the similarity as shown below.
    *
    */
  def calculateSchemaSimilarity = 1
  def calculateMetaDataSimilarity = 1


  /**
    * Filters the k best candidates from all potential candidates to extend the tree.
    *
    * @param k The number of candidates that should be kept.
    * @param candidates Original candidates
    * @return k best candidates
    */
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

  /**
    * Calculates score of the branch up to the (and including) the given node.
    *
    * @param node that represents the tree branch
    * @return total score of the branch
    */
  def getBranchScore(node: TreeNode): Score = node match {
    case Root(_) => 0f
    case Child(parent, _, _, _, score) => score + getBranchScore(parent)
  }

  /**
    * Generates candidates to extend the current tree by a layer. Each candidate represents a preparator applied to a
    * the intermediate dataset of the previous layer. The list of preparator classes allow us to instantiate the different preparators
    * and calculate their applicability scores.
    *
    * @param branchHead branchHead that could be extended by the generated candidates
    * @param preparators all possible preparators
    * @return candidates to extend the branch
    */
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

  /**
    * Generate the column combinations and exclude certain columns.
    *
    * @param df original dataframe
    * @param excludeCols excluded columns
    * @return the generated column combinations
    */
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
  /**
    * Maximal tree depth.
    */
  private val MAX_ITERATIONS = 100
  /**
    * Maximal number of branches to be kept. A higher number increases the performance but also leads to a higher runtime.
    */
  private val MAX_BRANCHES = 2
  /**
    * This threshold filters preparators with a lower applicability score.
    * We usually do not want preparators with a low score to be applied.
    */
  private val PREPARATOR_SCORE_THRESHOLD = 0.5

  type Score = Float

  /**
    * Tree datastructure
    */
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

  /**
    * Candidates to possibly extend the tree.
    */
  case class Candidate(branchHead: TreeNode, preparator: AbstractPreparator, affectedCols: Set[String], totalScore: Score, score: Score)

}
