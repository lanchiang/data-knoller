package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl.{MultiValueSeparator, SingleValueSeparator}
import de.hpi.isg.dataprep.preparators.implementation.SplitPropertyUtils.Separator
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable.ListBuffer

object DefaultSplitPropertyImpl {

  case class SingleValueSeparator(separatorValue: String) extends Separator {
    override def getNumSplits(value: String): Int =
      value.sliding(separatorValue.length).count(_ == separatorValue) + 1

    override def executeSplit(value: String): Vector[String] =
      value.split(separatorValue).toVector

    override def merge(split: Vector[String], original: String): String =
      split.mkString(separatorValue)
  }

  case class MultiValueSeparator(numCols: Int, globalDistribution: Map[String, Int]) extends Separator {
    override def getNumSplits(value: String): Int = {
      val (_, count) = SplitPropertyUtils.bestStringSeparatorCandidates(value, numCols)(0)
      count
    }

    override def executeSplit(value: String): Vector[String] = {
      val (separatorValue, _) = SplitPropertyUtils
        .bestStringSeparatorCandidates(value, numCols)
        .maxBy { case (candidate, _) => globalDistribution(candidate) }

      value.split(separatorValue).toVector
    }

    override def merge(split: Vector[String], original: String): String = {
      val (separatorValue, _) = SplitPropertyUtils
        .bestStringSeparatorCandidates(original, numCols)
        .maxBy { case (candidate, _) => globalDistribution(candidate) }

      split.mkString(separatorValue)
    }
  }

  case class CharacterClassSeparator(left: Char, right: Char) extends Separator {
    override def getNumSplits(value: String): Int = ???

    override def executeSplit(value: String): Vector[String] = ???

    override def merge(split: Vector[String], original: String): String = ???
  }

}

class DefaultSplitPropertyImpl extends AbstractPreparatorImpl {
  override def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {
    val parameters = abstractPreparator.asInstanceOf[SplitProperty]
    val resultDataFrame = try {
      executeSplitProperty(dataFrame, parameters)
    } catch {
      case e: Throwable =>
        errorAccumulator.add(new RecordError(parameters.propertyName, e))
        dataFrame
    }
    new ExecutionContext(resultDataFrame, errorAccumulator)
  }

  def executeSplitProperty(dataFrame: DataFrame, parameters: SplitProperty): DataFrame = {

    val test_input0="camelCaseTest"
    val test_input1="CamelCaseTest"
    val test_input2="112,83$"
    val test_input3="$77,75"


    Console.println(getCharacterClassSplitts(test_input0));
    Console.println(getCharacterClassSplitts(test_input1));
    Console.println(getCharacterClassSplitts(test_input2));
    Console.println(getCharacterClassSplitts(test_input3));

    val propertyName = parameters.propertyName
    if (!dataFrame.columns.contains(propertyName))
      throw new IllegalArgumentException(s"No column $propertyName found!")
    val column = dataFrame.select(propertyName).as(Encoders.STRING)

    val (separator, numCols) = (parameters.separator, parameters.numCols) match {
      case (None, None) =>
        val sep = findSeparator(column)
        val num = findNumCols(column, sep)
        (sep, num)
      case (Some(sep), None) =>
        val num = findNumCols(column, sep)
        (sep, num)
      case (None, Some(num)) =>
        val sep = findSeparator(column, num)
        (sep, num)
      case (Some(sep), Some(num)) =>
        (sep, num)
    }

    createSplitValuesDataFrame(
      dataFrame,
      propertyName,
      separator,
      numCols,
      parameters.fromLeft
    )
  }

  def findSeparator(column: Dataset[String]): Separator = {
    val stringSeparatorDistribution = SplitPropertyUtils.globalStringSeparatorDistribution(column)
    val separatorCandidates =
      stringSeparatorDistribution.keys.map(SingleValueSeparator).toList
    //TODO: ++ instances of all possible CharacterClassSeparators

    separatorCandidates.maxBy(evaluateSplit(column, _))
  }

  def findSeparator(column: Dataset[String], numCols: Int): Separator = {
    val stringSeparatorDistribution = SplitPropertyUtils.globalStringSeparatorDistribution(column, numCols)
    val separatorCandidates =
      MultiValueSeparator(numCols, stringSeparatorDistribution) ::
        stringSeparatorDistribution.keys.map(SingleValueSeparator).toList
    //TODO: ++ instances of all possible CharacterClassSeparators

    separatorCandidates.maxBy(evaluateSplit(column, _, numCols))
  }

  def findNumCols(column: Dataset[String], separator: Separator): Int = {
    // Given a column a separator, this method returns the number of columns that should be created by a split.
    // This number is the most common number of splits created by this separator throughout all rows
    import column.sparkSession.implicits._
    val numsSplits = column
      .map(separator.getNumSplits)
      .filter(x => x > 1)

    if (numsSplits.count() == 0)
      throw new IllegalArgumentException(s"Separator could not split any value.")

    numsSplits
      .groupByKey(identity)
      .mapGroups { case (numSplits, occurrences) => (numSplits, occurrences.length) }
      .collect
      .maxBy{case (_, numOccurrences) => numOccurrences}
      ._1
  }

  def evaluateSplit(column: Dataset[String], separator: Separator): Float = {
    import column.sparkSession.implicits._
    column
      .map(separator.getNumSplits)
      .groupByKey(identity)
      .mapGroups { case (_, occurrences) => occurrences.length }
      .collect.max.toFloat / column.count()
  }

  def evaluateSplit(column: Dataset[String], separator: Separator, numCols: Int): Float = {
    import column.sparkSession.implicits._
    column
      .map(separator.getNumSplits)
      .map(numSplits => numCols - Math.abs(numCols - numSplits))
      .map(rowScore => if (rowScore < 0) 0 else rowScore)
      .collect.sum.toFloat / numCols / column.count()
  }

  def createSplitValuesDataFrame(dataFrame: DataFrame, propertyName: String, separator: Separator, times: Int, fromLeft: Boolean): Dataset[Row] = {
    implicit val rowEncoder: Encoder[Row] = RowEncoder(appendEmptyColumns(dataFrame, propertyName, times).schema)
    dataFrame.map(
      row => {
        val index = row.fieldIndex(propertyName)
        val value = row.getAs[String](index)
        val split = separator.split(value, times, fromLeft)
        Row.fromSeq(row.toSeq ++ split)
      }
    )
  }

  def appendEmptyColumns(dataFrame: DataFrame, propertyName: String, numCols: Int): Dataset[Row] = {
    var result = dataFrame
    for (i <- 1 to numCols) {
      result = result.withColumn(s"$propertyName$i", lit(""))
    }
    result
  }

  /*
  Maps each character of the input string to its character class: letter (a), digit (1), whitespace (s) or special sign (the character itself)
   */

  def toCharacterClasses(input: String): Tuple2[String, String] = {
    val characterClasses = input.map(c => {
      if (Character.isDigit(c)) '1'
      else if (Character.isUpperCase(c)) 'A'
      else if (Character.isLowerCase(c)) 'a'
      else if (Character.isWhitespace(c)) 's'
      else c //
    })


    (characterClasses, input)
  }

  /*
  Removes duplicated characters in a row, e.g. fooboo becomes fobo
   */
  def reduceCharacterClasses(input: Tuple2[String, String]): Tuple3[String, String, String] = {
    var last = ' '
    var reduced = input._1.map(c => {
      var mapped = '\0'
      if (last != c) mapped = c
      last = c
      mapped
    })
    reduced=reduced.replace("\0", "");

    (reduced, input._1, input._2)
  }

  /*
  extracts all seperations which can used either used as splitt candidates or resulting splitt elements
   */
  def extractSeperatorCandidatesFromCharacterClasses(input: Tuple3[String, String, String]): List[String] = {



    var erg = new ListBuffer[String]
    var candidates = input._1.slice(1, input._1.length).distinct.toList

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

  def getCharacterClassTransitions(input: String): Set[String] = {
    var results = Set[String]()
    var characterClasses = reduceCharacterClasses(toCharacterClasses(input))
    var i=0
    while(i<characterClasses._1.length-1){
      results+=characterClasses._1.substring(i,i+2)
      i+=1;
    }
    results
  }

  /*
  returns the origin sequence for mapped and reduced string
   */
  def getOriginCharacters(input: Char, array: Tuple3[String, String, String]): Set[String] = {

    var results = Set[String]()
    val intermediate = array._2
    val origin = array._3

    var index = intermediate.indexOf(input)
    var allidx = new ListBuffer[Integer]
    while (index >= 0) {
      allidx += index
      index = intermediate.indexOf(input, index + 1)
    }

    var allidxs = allidx.toList


    var erg = ""

    var block = false
    var i = 0
    while (i < origin.length) {
      if (allidx.contains(i)) {
        erg += origin.charAt(i)
        if (i == origin.length - 1) results += erg
      }
      else {
        block = false
        if (erg.equals("") == false) results += erg
        erg = ""
      }


      i += 1;
      i - 1
    }

    results
  }

  def getOriginCharacters(inputIndex: Integer, array: Tuple3[String, String, String]): String = {

    var results = ""
    val reduced = array._1
    val intermediate = array._2
    val origin = array._3

    var index = inputIndex

    val characterClass=reduced.charAt(index)
    val numberOfCharacterClasses=reduced.count(_==characterClass)
    val thisIdxOfSameCharacters=reduced.substring(0,index).count(_==characterClass)

    //Console.println(numberOfCharacterClasses+" "+thisIdxOfSameCharacters)

  var i=0
    var block=false
    var metalist = ListBuffer[List[Integer]]()
    var list = ListBuffer[Integer]()
    while (i < intermediate.length) {
      if (characterClass==intermediate.charAt(i)) {
        list+=i

        if(i==intermediate.length-1)
          metalist+=list.toList
      }
      else {

        if(list.length>0){
          metalist+=list.toList
          list.clear()
        }
      }

      i += 1;
    }

    val idxlist=metalist.toList(thisIdxOfSameCharacters)
    val erg=idxlist.map(idx=>origin.charAt(idx)+"").reduce(_+_)
    erg
  }

  def addSplitteratorBetweenCharacterTransition(input: String, transition: String):String={
    val characterClasses = reduceCharacterClasses(toCharacterClasses(input))
    var i=0

    var erg=""
    var lastChar='z'
    while(i<characterClasses._1.length){
      val thisChar=  characterClasses._1.charAt(i)
      if(lastChar==transition.charAt(0)&&thisChar==transition.charAt(1))
        erg+=SplitPropertyUtils.defaultSplitterator

      erg+=getOriginCharacters(i,characterClasses)


      lastChar=thisChar
      i+=1
    }

    erg
  }

  def getCharacterClassCandidates(input: String): List[String] = {
    val res = filterFirstAndLastPartOut(extractSeperatorCandidatesFromCharacterClasses(reduceCharacterClasses(toCharacterClasses(input))), input)
    res
  }

  def getCharacterClassParts(input: String): List[String] = {
    val res = extractSeperatorCandidatesFromCharacterClasses(reduceCharacterClasses(toCharacterClasses(input)))
    res
  }

  def filterFirstAndLastPartOut(candidates: List[String], input: String): List[String] = {

    candidates.filter(candidate => {
      input.startsWith(candidate) == false && input.endsWith(candidate) == false
    })

  }

  def getCharacterClassSplitts(input: String): Set[String] = {
    val transitionCandidates=getCharacterClassTransitions(input)
    val filteredTransitionCandidates=transitionCandidates.filterNot(_.matches("Aa|1,|,1"))
    filteredTransitionCandidates.map(addSplitteratorBetweenCharacterTransition(input,_))
  }



}
