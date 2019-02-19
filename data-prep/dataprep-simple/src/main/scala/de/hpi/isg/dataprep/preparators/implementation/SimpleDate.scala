package de.hpi.isg.dataprep.preparators.implementation

import java.util.Locale

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex

case class LocalePattern(locale: Locale, pattern: String)

class SimpleDate(var originalDate: String, var yearOption: Option[String] = None, var monthOption: Option[String] = None,
                 var dayOption: Option[String] = None, var dayOfTheWeekOption: Option[String] = None, var separators: List[String] = List()) extends Serializable {

  //-------------------Constructor--------------------------
  val splitDate: List[String] = originalDate.split(AdaptiveDateUtils.nonAlphaNumericPattern).toList
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
      } else if (AdaptiveDateUtils.isNumber(datePart)) {
        undeterminedBlocks.append(datePart)
      }
    }
    applyNumberRules(undeterminedBlocks.toList)

    if (allPartsDefined) {
      val resultingPattern = generatePattern()
      // Take first locale. Assumption: There should only be one locale per date. Default to US, if none is found
      val maybeLocale = alreadyFoundPatterns.flatten.headOption
      val locale: Locale = if (maybeLocale.isDefined) maybeLocale.get.locale else Locale.US

      println(s"Result: $resultingPattern\n")
      return Some(LocalePattern(locale, resultingPattern))
    }
    None
  }

  def applyNumberRules(undeterminedBlocks: List[String]): Unit = {
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

    var newGroup = splitDate.updated(splitDate.indexOf(year), generatePlaceholder(year, "y"))
    newGroup = newGroup.updated(newGroup.indexOf(month), generatePlaceholder(month, "M"))
    newGroup = newGroup.updated(newGroup.indexOf(day), generatePlaceholder(day, "d"))

    if (dayOfTheWeekOption.isDefined) {
      val dayOfTheWeek = dayOfTheWeekOption.get

      newGroup = newGroup.updated(newGroup.indexOf(dayOfTheWeek), generatePlaceholder(dayOfTheWeek, "E"))
    }

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
      if (groupAsString.startsWith("EEEE")) {
        groupAsString = "EEEE"
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

//  def allPartsDefined(alreadyFoundPatterns: List[Option[LocalePattern]]): Boolean = {
  def allPartsDefined: Boolean = {
    dayOption.isDefined && monthOption.isDefined && yearOption.isDefined
  }

  def startsWithSeparator: Boolean = {
    originalDate.matches(AdaptiveDateUtils.startsWithSeparatorPattern)
  }

  def getSeparators: List[String] = {
    val separatorPattern: Regex = AdaptiveDateUtils.nonAlphaNumericPattern.r
    separatorPattern.findAllMatchIn(originalDate).map(_.toString).toList
  }

  override def toString: String ={
    s"$yearOption, $monthOption, $dayOption"
  }
}
