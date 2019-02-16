package de.hpi.isg.dataprep.preparators.implementation

import org.apache.spark.sql.Dataset

import scala.collection.mutable.ListBuffer

object SplitPropertyUtils {
  val defaultSplitterator = "#!#"
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
    val sameDiffCandidates = allStringSeparatorCandidates(value)
      .groupBy { case (_, count) => Math.abs(count + 1 - numCols) }
      .minBy { case (diff, _) => diff }
      ._2
    val maxLength = sameDiffCandidates.map{case (candidate, _) => candidate.length}.max
    sameDiffCandidates.filter{ case (candidate, _) => candidate.length == maxLength}
  }

  def allCharacterClassCandidates(value: String): Vector[(String, Int)] = {
    getCharacterClassTransitions(value)
  }

  def bestCharacterClassSeparatorCandidates(value: String, numCols: Int): Vector[(String, Int)] = {
    getCharacterClassTransitions(value)
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

  def globalTransitionSeparatorDistribution(dataFrame: Dataset[String], numCols: Int): Map[String, Int] = {
    import dataFrame.sparkSession.implicits._
    dataFrame
      .flatMap(value =>
        bestCharacterClassSeparatorCandidates(value, numCols)
          .filter { case (_, count) => count + 1 == numCols }
          .map { case (candidate, _) => candidate }
      )
      .groupByKey(identity)
      .mapGroups { case (candidate, occurrences) => (candidate, occurrences.length) }
      .collect()
      .toMap
  }

  def globalTransitionSeparatorDistribution(dataFrame: Dataset[String]): Map[String, Int] = {
    import dataFrame.sparkSession.implicits._
    dataFrame
      .flatMap(value => getCharacterClassTransitions(value).map { case (candidate, _) => candidate })
      .groupByKey(identity)
      .mapGroups { case (candidate, occurrences) => (candidate, occurrences.length) }
      .collect()
      .filter { case (candidate, _) => !candidate.equals("Aa") }
      .toMap
  }

  def toCharacterClasses(input: String): (String, String) = {
    val characterClasses = input.map(c => {
      if (Character.isDigit(c)) '1'
      else if (Character.isUpperCase(c)) 'A'
      else if (Character.isLowerCase(c)) 'a'
      else if (Character.isWhitespace(c)) 's'
      else '.'
    })


    (characterClasses, input)
  }

  /*
  Removes duplicated characters in a row, e.g. fooboo becomes fobo
   */
  def reduceCharacterClasses(input: (String, String)): (String, String, String) = {
    var last = '\0'
    var reduced = input._1.map(c => {
      var mapped = '\0'
      if (last != c) mapped = c
      last = c
      mapped
    })
    reduced = reduced.replace("\0", "")

    (reduced, input._1, input._2)
  }

  /*
  extracts all seperations which can used either used as splitt candidates or resulting splitt elements
   */
  def extractSeperatorCandidatesFromCharacterClasses(input: (String, String, String)): List[String] = {
    var erg = new ListBuffer[String]
    val candidates = input._1.slice(1, input._1.length).distinct.toList

    if (candidates.isEmpty) {
      val start = getOriginCharacters(input._1.charAt(0), input)
      val end = getOriginCharacters(input._1.charAt(input._1.length - 1), input)

      start.foreach(str => {
        erg += str
      })

      end.foreach(str => {
        erg += str
      })
    }
    else {
      candidates + input._1.charAt(input._1.length - 1).toString

      candidates.foreach(candidate => {
        val elems = getOriginCharacters(candidate, input)
        elems.foreach(str => {
          erg += str
        })
      })
    }

    erg.toList
  }

  def getCharacterClassTransitions(input: String): Vector[(String, Int)] = {
    var results = ListBuffer[String]()
    val characterClasses = reduceCharacterClasses(toCharacterClasses(input))
    var i = 0
    while (i < characterClasses._1.length - 1) {
      results += characterClasses._1.substring(i, i + 2)
      i += 1
    }

    results
      .groupBy(identity)
      .mapValues(_.size)
      .toVector
  }

  /*
  returns the origin sequence for mapped and reduced string
   */
  def getOriginCharacters(input: Char, array: (String, String, String)): Set[String] = {
    var results = Set[String]()
    val intermediate = array._2
    val origin = array._3
    var index = intermediate.indexOf(input)
    var allidx = new ListBuffer[Integer]

    while (index >= 0) {
      allidx += index
      index = intermediate.indexOf(input, index + 1)
    }

    var erg = ""
    var i = 0
    while (i < origin.length) {
      if (allidx.contains(i)) {
        erg += origin.charAt(i)
        if (i == origin.length - 1) results += erg
      }
      else if (!erg.equals("")) {
        results += erg
        erg = ""
      }
      i += 1
    }
    results
  }

  def getOriginCharacters(index: Integer, array: (String, String, String)): String = {
    val reduced = array._1
    val intermediate = array._2
    val origin = array._3

    val characterClass = reduced.charAt(index)
    val thisIdxOfSameCharacters = reduced.substring(0, index).count(_ == characterClass)

    var i = 0
    var metalist = ListBuffer[List[Integer]]()
    var list = ListBuffer[Integer]()
    while (i < intermediate.length) {
      if (characterClass == intermediate.charAt(i)) {
        list += i
        if (i == intermediate.length - 1)
          metalist += list.toList
      }
      else if (list.nonEmpty) {
        metalist += list.toList
        list.clear()
      }
      i += 1
    }

    val idxlist = metalist.toList(thisIdxOfSameCharacters)
    val erg = idxlist.map(idx => origin.charAt(idx) + "").reduce(_ + _)
    erg
  }

  def addSplitteratorBetweenCharacterTransition(input: String, transition: String): String = {
    val characterClasses = reduceCharacterClasses(toCharacterClasses(input))
    var i = 0
    var erg = ""
    var lastChar = 'z'

    while (i < characterClasses._1.length) {
      val thisChar = characterClasses._1.charAt(i)
      if (lastChar == transition.charAt(0) && thisChar == transition.charAt(1))
        erg += SplitPropertyUtils.defaultSplitterator

      erg += getOriginCharacters(i, characterClasses)
      lastChar = thisChar
      i += 1
    }
    erg
  }

  def getCharacterClassCandidates(input: String): List[String] = {
    val res = filterFirstAndLastPartOut(
      extractSeperatorCandidatesFromCharacterClasses(
        reduceCharacterClasses(
          toCharacterClasses(
            input
          )
        )
      ),
      input
    )
    res
  }

  def getCharacterClassParts(input: String): List[String] = {
    val res = extractSeperatorCandidatesFromCharacterClasses(
      reduceCharacterClasses(
        toCharacterClasses(
          input
        )
      )
    )
    res
  }

  def filterFirstAndLastPartOut(candidates: List[String], input: String): List[String] = {
    candidates.filter(candidate => {
      !(input.startsWith(candidate) || input.endsWith(candidate))
    })

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
