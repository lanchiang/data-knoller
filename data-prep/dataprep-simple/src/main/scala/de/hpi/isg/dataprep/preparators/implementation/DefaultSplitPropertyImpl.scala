package de.hpi.isg.dataprep.preparators.implementation

import de.hpi.isg.dataprep.ExecutionContext
import de.hpi.isg.dataprep.components.AbstractPreparatorImpl
import de.hpi.isg.dataprep.model.error.{PreparationError, RecordError}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Encoder, Row}
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class DefaultSplitPropertyImpl extends AbstractPreparatorImpl {

  def evaluateSplit(column: Dataset[String], separator: String, numCols: Int): Float = {
    import column.sparkSession.implicits._
    column
      .map(value => value.sliding(separator.length).count(_ == separator) + 1)
      .map(numSplits => numCols - Math.abs(numCols - numSplits))
      .map(rowScore => if(rowScore < 0)  0 else rowScore)
      .collect()
      .sum.toFloat / numCols / column.count()
  }

  override def executeLogic(abstractPreparator: AbstractPreparator, dataFrame: Dataset[Row], errorAccumulator: CollectionAccumulator[PreparationError]): ExecutionContext = {

    val preparator = abstractPreparator.asInstanceOf[SplitProperty]
    val propertyName = preparator.propertyName

    val parameters = for {
      separator <- Try(
        preparator.separator.getOrElse(
          findSeparator(dataFrame, propertyName)
        )
      )
      numCols <- Try(
        preparator.numCols.getOrElse(
          findNumberOfColumns(dataFrame, propertyName, separator)
        )
      )
    } yield (separator, numCols)

    val returnDataFrame = parameters match {
      case Success((separator, numCols)) =>
        val splitDataFrame = createSplitValuesDataFrame(
          dataFrame,
          propertyName,
          separator,
          numCols,
          preparator.fromLeft
        )
        splitDataFrame.count()
        splitDataFrame
      case Failure(e: IllegalArgumentException) =>
        errorAccumulator.add(new RecordError(propertyName, e))
        dataFrame
      case Failure(e) => throw e
    }

    new ExecutionContext(returnDataFrame, errorAccumulator)
  }

  def findSeparator(dataFrame: Dataset[Row], propertyName: String): String = {
    // Given a column name, this method returns the character that is most likely the separator.
    // Possible separators are non-alphanumeric characters that are evenly distributed over all rows.
    // Rows where the character does not appear at all are ignored.
    // If multiple characters fulfill this condition, the most common character is selected.

    if (!dataFrame.columns.contains(propertyName))
      throw new IllegalArgumentException(s"No column $propertyName found!")

    val charMaps = dataFrame.select(propertyName).collect().map(
      row => {
        val value = row.getAs[String](0)
        value.groupBy(identity).filter {
          case (char, string) => !char.isLetterOrDigit
        }.mapValues(_.length)
      })
    val chars = charMaps.flatMap(map => map.keys).distinct

    val checkSeparatorCondition = (char: Char) => {
      val counts = charMaps.map(map => map.withDefaultValue(0)(char)).filter(x => x > 0)
      (counts.forall(_ == counts.head), counts.head, char)
    }
    val candidates = chars.map(checkSeparatorCondition).filter { case (valid, counts, char) => valid }

    if (candidates.isEmpty)
      throw new IllegalArgumentException(s"No possible separator found in column $propertyName")
    candidates.maxBy { case (valid, counts, char) => counts }._3.toString
  }

  def findNumberOfColumns(dataFrame: Dataset[Row], propertyName: String, separator: String): Int = {
    // Given a column name, and a separator, this method returns the number of columns that should be
    // created by a split. This number is the maximum of the number of split parts over all rows.

    val counts = dataFrame.select(propertyName).collect().map(
      row => {
        val value = row.getAs[String](0)
        value.split(separator).length
      }
    ).filter(x => x > 1)

    if (counts.isEmpty)
      throw new IllegalArgumentException(s"Separator not found in column $propertyName")
    counts.max
  }

  def createSplitValuesDataFrame(dataFrame: Dataset[Row], propertyName: String, separator: String, times: Int, fromLeft: Boolean): Dataset[Row] = {
    if (!dataFrame.columns.contains(propertyName))
      throw new IllegalArgumentException(s"No column $propertyName found!")

    val splitValue = (value: String) => {
      var split = value.split(separator)
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
      head :+ tail.mkString(separator)
    }

    val rowEncoder: Encoder[Row] = RowEncoder(appendEmptyColumns(dataFrame, propertyName, times).schema)
    dataFrame.map(
      row => {
        val index = row.fieldIndex(propertyName)
        val value = row.getAs[String](index)
        val split = splitValue(value)
        Row.fromSeq(row.toSeq ++ split)
      }
    )(rowEncoder)
  }

  def appendEmptyColumns(dataFrame: Dataset[Row], propertyName: String, numCols: Int): Dataset[Row] = {
    var result = dataFrame
    for (i <- 1 to numCols) {
      result = result.withColumn(s"$propertyName$i", lit(""))
    }
    result
  }



  /*
  Maps each character of the input string to its character class: letter (a), digit (1), whitespace (s) or special sign (the character itself)
   */

  def toCharacterClasses(input: String):Tuple2[String,String]={
    val characterClasses=input.map(c=>{
      if (Character.isDigit(c))'1'
      else if (Character.isUpperCase(c)) 'a'
      else if (Character.isLowerCase(c)) 'a'
      else if (Character.isWhitespace(c)) 's'
      else '.'//c //
    })


    (characterClasses, input)
  }

  /*
  Removes duplicated characters in a row, e.g. fooboo becomes fobo
   */
  def reduceCharacterClasses(input: Tuple2[String,String]): Tuple3[String,String,String]={
    var last=' '
    val reduced=input._1.map(c=>{
      var mapped='\0'
      if(last!=c) mapped=c
      last=c
      mapped
    })


    (reduced,input._1,input._2)
  }

  /*
  extracts all seperations which can used either used as splitt candidates or resulting splitt elements
   */
  def extractSeperatorCandidates(input: Tuple3[String, String, String]):List[String]={

    var erg = new ListBuffer[String]
    var candidates=input._1.slice(1,input._1.length).distinct.toList

    if(candidates.isEmpty){
     val start=getOriginCharacters(input._1.charAt(0),input)
     val end=getOriginCharacters(input._1.charAt(input._1.length-1),input)

      start.foreach(str=>{
        erg+=str
      })

      end.foreach(str=>{
        erg+=str
      })
    }
    else{
      candidates+input._1.charAt(input._1.length-1).toString

      candidates.foreach(candidate=>{
        val elems=getOriginCharacters(candidate,input)
        elems.foreach(str=>{
          erg+=str
        })
      })
    }

    erg.toList
  }

  /*
  returns the origin sequence for mapped and reduced string
   */
  def getOriginCharacters(input: Char, array:Tuple3[String,String,String]):Set[String]={

    var results=Set[String]()
    val intermediate=array._2
    val origin=array._3

    var index = intermediate.indexOf(input)
    var allidx= new ListBuffer[Integer]
    while (index >= 0) {
      allidx += index
      index = intermediate.indexOf(input, index + 1)
    }

    var allidxs=allidx.toList


    var erg = ""

    var block = false
    var i = 0
    while (i < origin.length) {
      if (allidx.contains(i)) {
        erg += origin.charAt(i)
        if (i == origin.length - 1) results+=erg
      }
      else {
        block = false
        if (erg.equals("") == false) results+=erg
        erg = ""
      }


      i += 1; i - 1
    }

    results
  }

  def getCandidates(input: String):List[String]={
    val res=filterFirstAndLastPartOut(extractSeperatorCandidates(reduceCharacterClasses(toCharacterClasses(input))),input)
    res
  }

  def getParts(input: String):List[String]={
    val res=extractSeperatorCandidates(reduceCharacterClasses(toCharacterClasses(input)))
    res
  }

  def filterFirstAndLastPartOut(candidates: List[String], input: String):List[String]={

    candidates.filter(candidate=>{
      input.startsWith(candidate) == false&&input.endsWith(candidate) == false
    })

  }



}
