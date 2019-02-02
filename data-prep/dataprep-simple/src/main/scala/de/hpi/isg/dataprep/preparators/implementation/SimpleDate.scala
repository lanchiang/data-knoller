package de.hpi.isg.dataprep.preparators.implementation

import scala.util.matching.Regex

class SimpleDate(var originalDate: String, var yearOption: Option[String] = None, var monthOption: Option[String] = None,
                 var dayOption: Option[String] = None, var separators: List[String] = List()) {
  //-------------------Constructor--------------------------
  val splitDate: List[String] = originalDate.split("[^0-9a-zA-z]{1,}").toList
  println(s"Splits: $splitDate")

  println("Separators: " + getSeparators + " starts with separator: " + startsWithSeparator)

  val (leftoverBlocks, convertedMonth) = convertMonthNames(splitDate)
  var undeterminedBlocks: List[String] = padSingleDigitDates(leftoverBlocks)

  // Used for patternGeneration later
  val monthText: Option[String] =  splitDate.find(!leftoverBlocks.contains(_))

  monthOption = convertedMonth
  println(s"-> Month: $monthOption")

  if (isMonthDefined) {
    undeterminedBlocks = undeterminedBlocks.filter(_ != monthOption.get)
  }
  //--------------------------------------------------------

  def isYearDefined: Boolean = {
    yearOption.isDefined
  }

  def isMonthDefined: Boolean = {
    monthOption.isDefined
  }

  def isDayDefined: Boolean = {
    dayOption.isDefined
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

  // TODO: check if needed after locale rework
  private def convertMonthNames(splittedDate: List[String]): (List[String], Option[String]) = {
    val monthNameToNumber = Map(
      "January"   -> 1,
      "February"  -> 2,
      "March"     -> 3,
      "April"     -> 4,
      "May"       -> 5,
      "June"      -> 6,
      "July"      -> 7,
      "August"    -> 8,
      "September" -> 9,
      "October"   -> 10,
      "November"  -> 11,
      "December"  -> 12
    )
    var convertedMonth: String = ""
    var newDate: List[String] = splittedDate

    for ((block, index) <- splittedDate.view.zipWithIndex) {
      if (block.forall(_.isLetter)) {
        for (month <- monthNameToNumber.keys) {
          if (month.toLowerCase().startsWith(block.toLowerCase())) {
            // TODO: Early abort may lead to errors if multiple strings are present, e.g. DoW and month
            convertedMonth = f"${monthNameToNumber(month)}%02d"
            newDate = newDate.updated(index, convertedMonth)
            return (newDate, Some(convertedMonth))
          }
        }
      }
    }

    (newDate, None)
  }

  private def padSingleDigitDates(dates: List[String]): List[String] = {
    dates.map( date => if (date.length == 1 && date.forall(Character.isDigit)) "0" + date else date )
  }

  override def toString: String ={
    s"$yearOption, $monthOption, $dayOption"
  }
}
