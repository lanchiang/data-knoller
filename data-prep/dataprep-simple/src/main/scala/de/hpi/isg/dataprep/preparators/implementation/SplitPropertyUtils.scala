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
    val allCandidates = allStringSeparatorCandidates(value)
    if (allCandidates.isEmpty) {
      throw new IllegalArgumentException("Could not find any string separator candidates.")
    }
    val sameDiffCandidates = allStringSeparatorCandidates(value)
      .groupBy { case (_, count) => Math.abs(count + 1 - numCols) }
      .minBy { case (diff, _) => diff }
      ._2
    val maxLength = sameDiffCandidates.map { case (candidate, _) => candidate.length }.max
    sameDiffCandidates.filter { case (candidate, _) => candidate.length == maxLength }
  }

  /*
  equivalent to getCharacterClassTransitions(value: String)
   */
  def allCharacterClassCandidates(value: String): Vector[(String, Int)] = {
    getCharacterClassTransitions(value)
  }

  def bestCharacterClassSeparatorCandidates(value: String, numCols: Int): Vector[(String, Int)] = {
    val allCandidates = getCharacterClassTransitions(value)
    if (allCandidates.isEmpty) {
      throw new IllegalArgumentException("Could not find any character class separator candidates.")
    }
    allCandidates
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


  /*
    Transforms input string into its CharacterClasses, e.g. 'DataKnoller' becomes 'AaaaAaaaaaa'
    returns tuple with (CharacterClassesString, OriginalString)
   */
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
  Removes duplicated characters in a row, e.g. 'fooboo' becomes 'fobo'
   returns tuple with (ReducedCharacterClassesString, CharacterClassesString, OriginalString)
   */
  def reduceCharacterClasses(input: (String, String)): (String, String, String) = {
    //map each character to '\0'
    var last = '\0'
    var reduced = input._1.map(c => {
      var mapped = '\0'
      if (last != c) mapped = c
      last = c
      mapped
    })
    //replace all '\0' with nothing
    reduced = reduced.replace("\0", "")

    (reduced, input._1, input._2)
  }


  /*
    Extracts the transitions between CharacterClasses, eg. find for String 'DataPrep' the vector ((Aa,2),(aA,1))
   */
  def getCharacterClassTransitions(input: String): Vector[(String, Int)] = {
    var results = ListBuffer[String]()
    val characterClasses = reduceCharacterClasses(toCharacterClasses(input))
    var i = 0
    //apply sliding window of length 2 on ReducedClassesString
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
   Returns the origin sequence for the CharacterClass on the specified index. CharacterClass is passed as Tuple containing the Transformation History.
   'AaAa' in tuple ('AaAa','AaaaAaaa','DataPrep')with given index 1 becomes 'aaa' in CharacterClass representation and finally 'ata'. With index 4 it becomes 'rep'
   */
  def getOriginCharacters(index: Integer, array: (String, String, String)): String = {
    val reduced = array._1
    val intermediate = array._2
    val origin = array._3

    //get CharacterClass at specified index
    val characterClass = reduced.charAt(index)
    //get indices of the characterclass of specified index
    val thisIdxOfSameCharacters = reduced.substring(0, index).count(_ == characterClass)

    var i = 0
    var metalist = ListBuffer[List[Integer]]()
    var list = ListBuffer[Integer]()

    //iterate over CharacterClass representation to restore indices of character class.
    // 'AaAa' with given index 1 becomes 'aaa' in CharacterClass repräsentation 'DataPrep' with indices [1,2,3]
    while (i < intermediate.length) {
      if (characterClass == intermediate.charAt(i)) {
        list += i

        if (i == intermediate.length - 1)
          metalist += list.toList
      }
      else {

        if (list.nonEmpty) {
          metalist += list.toList
          list.clear()
        }
      }
      i += 1
    }

    val idxlist = metalist.toList(thisIdxOfSameCharacters)

    //map each idx of the CharacterClass representation to its origin character
    val erg = idxlist.map(idx => origin.charAt(idx) + "").reduce(_ + _)
    erg
  }

  /*
  returns for a string and a given CharacterClass Transition a String with default splitterator Charactor between the transition.
  'DataPrep' with transition 'aA' returns 'Data|Prep',
  'DataPrep' with transition 'Aa' returns 'D|ataP|rep'
   */
  def addSplitteratorBetweenCharacterTransition(input: String, transition: String): String = {
    val characterClasses = reduceCharacterClasses(toCharacterClasses(input))
    var i = 0

    var erg = ""
    var lastChar = 'z' //is no characterclass and this why valid

    //iterates over characterclass string and adds spliterator between the inpüut transition
    while (i < characterClasses._1.length) {
      val thisChar = characterClasses._1.charAt(i)
      if (lastChar == transition.charAt(0) && thisChar == transition.charAt(1))
        erg += SplitPropertyUtils.defaultSplitterator
      //maps to originalCharacters
      erg += getOriginCharacters(i, characterClasses)
      lastChar = thisChar
      i += 1
    }

    erg
  }

  abstract class Separator() {
    def mostLikelySeparator: String

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
