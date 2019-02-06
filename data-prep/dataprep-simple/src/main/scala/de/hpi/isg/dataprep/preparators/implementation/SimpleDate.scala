package de.hpi.isg.dataprep.preparators.implementation

import java.util.Locale

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex

case class LocalePattern(locale: Locale, pattern: String)

class SimpleDate(var originalDate: String, var yearOption: Option[String] = None, var monthOption: Option[String] = None,
                 var dayOption: Option[String] = None, var dayOfTheWeekOption: Option[String] = None, var separators: List[String] = List()) extends Serializable {

  //-------------------Constructor--------------------------
  val splitDate: List[String] = originalDate.split("[^0-9a-zA-z]{1,}").toList
  println(s"Splits: $splitDate")

  //--------------------------------------------------------

  def toPattern(alreadyFoundPatterns: List[Option[LocalePattern]]): Option[LocalePattern] = {
    val undeterminedBlocks = ListBuffer[String]()
    // Use already found text patterns
    (splitDate zip alreadyFoundPatterns).foreach{case (datePart, maybeLocalePattern) =>
      if (maybeLocalePattern.isDefined) {
        if (maybeLocalePattern.get.pattern == "MMM") {
          monthOption = Some(datePart)
        } else if (maybeLocalePattern.get.pattern == "EEE") {
          dayOfTheWeekOption = Some(datePart)
        }
      } else {
        undeterminedBlocks.append(datePart)
      }
    }
    applyRules(undeterminedBlocks.toList)

    if (isDefined) {
      val resultingPattern = generatePattern()
      // Take first locale. Assumption: There should only be one locale per date. Default to US, if none is found
      val maybeLocale = alreadyFoundPatterns.flatten.headOption
      val locale: Locale = if (maybeLocale.isDefined) maybeLocale.get.locale else Locale.US

      println(s"Result: $resultingPattern\n")
      return Some(LocalePattern(locale, resultingPattern))
    }
    println("")
    None
  }

  def applyRules(undeterminedBlocks: List[String]): Unit = {
    // TODO: If multiple blocks are the same, it wouldn't matter which is which
    var leftoverBlocks: List[String] = undeterminedBlocks

    for (block <- undeterminedBlocks) { // if there are no undetermined parts left, function will return
      breakable {
        if (block.toInt <= 0) {
          break
        }
        if (!isYearDefined && (block.length == 4 || block.toInt > 31 || (isDayDefined && block.toInt > 12))) {
          yearOption = Some(block)
        } else if (!isDayDefined && (block.toInt > 12 && block.toInt <= 31 ||
          (isMonthDefined && block.toInt > 0 && block.toInt <= 31))) {
          dayOption = Some(block)
        } else {
          break
        }
        leftoverBlocks = leftoverBlocks.filter(_ != block)
      }
    }

    // assign leftover block if there is any
    if (leftoverBlocks.length == 1) {
      if (!isYearDefined) {
        yearOption = Some(leftoverBlocks.head)
      } else if (!isMonthDefined) {
        monthOption = Some(leftoverBlocks.head)
      } else if (!isDayDefined) {
        dayOption = Some(leftoverBlocks.head)
      }
    }
  }

  def generatePlaceholder(origString:String, placeholder:String): String = {
    origString.replaceAll(".", placeholder)
  }

  def generatePattern(): String = {
    var pattern: String = ""
    val year = yearOption.get
    val month = monthOption.get
    val day = dayOption.get
    println(s"generatePattern: $getSeparators, $year, $month, $day")

    var newGroup = splitDate.updated(splitDate.indexOf(year), generatePlaceholder(year, "y"))
    newGroup = newGroup.updated(newGroup.indexOf(month), generatePlaceholder(month, "M"))
    newGroup = newGroup.updated(newGroup.indexOf(day), generatePlaceholder(day, "d"))

    val separatorsBuffer: ListBuffer[String] = getSeparators.to[ListBuffer]

    if (startsWithSeparator) {
      // Pop first element
      pattern = separatorsBuffer.remove(0)
      println(getSeparators)
    }

    for(group <- newGroup) {
      var groupAsString = group.toString

      // Full text pattern for month is MMMM
      if (groupAsString.startsWith("MMMM")) {
        groupAsString = "MMMM"
      }

      val separator = if(separatorsBuffer.nonEmpty) separatorsBuffer.remove(0) else ""

      pattern = pattern + groupAsString + separator
    }

    pattern
  }

  def isYearDefined: Boolean = {
    yearOption.isDefined
  }

  def isMonthDefined: Boolean = {
    monthOption.isDefined
  }

  def isDayDefined: Boolean = {
    dayOption.isDefined
  }

  def isDayOfTheWeekDefined: Boolean = {
    dayOfTheWeekOption.isDefined
  }

  def isDefined: Boolean = {
    dayOption.isDefined && monthOption.isDefined && yearOption.isDefined
  }

  def startsWithSeparator: Boolean = {
    val startsWithSeparatorPattern: String = "^[^0-9a-zA-z]{1,}"
    originalDate.matches(startsWithSeparatorPattern)
  }

  def getSeparators: List[String] = {
    val separatorPattern: Regex = "[^0-9a-zA-z]{1,}".r
    separatorPattern.findAllMatchIn(originalDate).map(_.toString).toList
  }

  private def padSingleDigitDates(dates: List[String]): List[String] = {
    dates.map( date => if (date.length == 1 && date.forall(Character.isDigit)) "0" + date else date )
  }

  override def toString: String ={
    s"$yearOption, $monthOption, $dayOption"
  }
}
