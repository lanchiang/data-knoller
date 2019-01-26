package de.hpi.isg.dataprep.preparators.implementation

import org.apache.spark.sql.Dataset

object SplitPropertyUtils {
  private val nonAlpaLong = """[^a-zA-Z\d]+""".r
  private val nonAlpaShort = """[^a-zA-Z\d]""".r

  def allStringSeparatorCandidates(value: String): Vector[(String, Int)] = {
    (nonAlpaLong.findAllIn(value) ++ nonAlpaShort.findAllIn(value))
      .toVector
      .groupBy(identity)
      .mapValues(_.length)
      .toVector
  }

  def bestStringSeparatorCandidates(value: String, numCols: Int): Vector[(String, Int)] = {
    allStringSeparatorCandidates(value)
      .groupBy { case (candidate, count) => Math.abs(count + 1 - numCols) }
      .minBy { case (diff, candidates) => diff }
      ._2
  }

  def globalStringSeparatorDistribution(dataFrame: Dataset[String], numCols: Int): Map[String, Int] = {
    import dataFrame.sparkSession.implicits._
    dataFrame
      .flatMap(value =>
        bestStringSeparatorCandidates(value, numCols)
          .filter{case (candidate, count) => count + 1 == numCols}
          .map{case (candidate, count) => candidate}
      )
      .groupByKey(identity)
      .mapGroups{case (candidate, occurences) => (candidate, occurences.length)}
      .collect()
      .toMap
  }

  abstract class Separator() {
    def getNumSplits(value: String): Int

    def merge(split: Vector[String], original: String): String

    def executeSplit(value: String): Vector[String]

    def split(value: String, times: Int, fromLeft: Boolean): Vector[String] = {
      var split = this.executeSplit(value)
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
      head :+ this.merge(tail, original = value)
    }
  }

}
