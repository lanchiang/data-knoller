package de.hpi.isg.dataprep.preparators.implementation

import org.apache.spark.sql.Dataset

object SplitPropertyUtils {
  val defaultSplitterator="#!#"
  private val nonAlpaShort = """[^a-zA-Z\d]""".r
  private val nonAlpaLong = """[^a-zA-Z\d]{2,}""".r

  def allStringSeparatorCandidates(value: String): Vector[(String, Int)] = {
    (nonAlpaLong.findAllIn(value) ++ nonAlpaShort.findAllIn(value))
      .toVector
      .groupBy(identity)
      .mapValues(_.length)
      .toVector
  }

  def bestStringSeparatorCandidates(value: String, numCols: Int): Vector[(String, Int)] = {
    allStringSeparatorCandidates(value)
      .groupBy { case (_, count) => Math.abs(count + 1 - numCols) }
      .minBy { case (diff, _) => diff }
      ._2
  }

  def globalStringSeparatorDistribution(dataFrame: Dataset[String], numCols: Int): Map[String, Int] = {
    import dataFrame.sparkSession.implicits._
    dataFrame
      .flatMap(value =>
        bestStringSeparatorCandidates(value, numCols)
          .filter { case (_, count) => count + 1 == numCols }
          .map { case (candidate, _) => candidate }
      )
      .groupByKey(identity)
      .mapGroups { case (candidate, occurrences) => (candidate, occurrences.length) }
      .collect()
      .toMap
  }

  def globalStringSeparatorDistribution(dataFrame: Dataset[String]): Map[String, Int] = {
    import dataFrame.sparkSession.implicits._
    dataFrame
      .flatMap(value => allStringSeparatorCandidates(value).map { case (candidate, _) => candidate })
      .groupByKey(identity)
      .mapGroups { case (candidate, occurrences) => (candidate, occurrences.length) }
      .collect()
      .toMap
  }

  abstract class Separator() {
    def getNumSplits(value: String): Int

    def merge(split: Vector[String], original: String): String

    def executeSplit(value: String): Vector[String]

    def split(value: String, times: Int, fromLeft: Boolean): Vector[String] = {
      var split = executeSplit(value)
      if (!fromLeft)
        split = split.reverse

      if (split.length <= times) {
        val fill = List.fill(times - split.length)("")
        split = split ++ fill
      }

      val head = split.slice(0, times - 1)
      var tail = split.slice(times - 1, split.length)
      if (!fromLeft) {
        tail = tail.reverse
      }
      head :+ merge(tail, original = value)
    }
  }
}
