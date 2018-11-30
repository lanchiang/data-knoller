import scala.util.matching.Regex

// Concept:
// Separators are either "/",".","-" or no separator at all
// Always 3 groups of numbers which are either 1/2 digits long (day/month) or 2/4 (year)
// Year either in front or at the back otherwise stupid
// Single digits == day or month

// Additional stuff:
// Some months have less days
// Months could be represented as Stings, such as "Jan", "Oct"

val possible_dates = List(
  "12.03.2004",
  "31.1.16",
  "25.1.16",
  "9.1.16",
  "04/28/1996",
  "23.3.98",
  "09/17/96",
  "1975-01-18",
  "75-01-18",
  "75 01 18",
  "20020330",
  "29 Jul. 1935"
)

def generatePlaceholder(origString:String, placeholder:String):String = {
  origString.replaceAll(".", placeholder)
}

def generatePatternAndRegex(fullGroup:List[String], separators:List[String], year: String, month: String, day: String): (String, String) = {
  var newGroup = fullGroup.updated(fullGroup.indexOf(year), generatePlaceholder(year, "Y"))
  newGroup = newGroup.updated(newGroup.indexOf(month), generatePlaceholder(month, "M"))
  newGroup = newGroup.updated(newGroup.indexOf(day), generatePlaceholder(day, "D"))

  var pattern: String = ""
  var regex: String = ""

  for(group <- newGroup) {
    val groupAsString = group.toString

    pattern = pattern + groupAsString

    "[DMY]+".r.findFirstMatchIn(groupAsString) match {
      case Some(_) =>
        regex = regex + s"[0-9]{${groupAsString.length}}"
      case None =>
        regex = regex + "\\" + groupAsString // TODO: better escaping
    }

  }

  (regex, pattern)
}

def determineDateAndMonth(groups: List[String]): (String, String) = {
  var day: String = "XX"
  var month: String = "XX"

  groups.find { group =>
    group.toInt > 12
  } match {
    case Some(group) =>
      day = group
      month = groups.filter(_ != group).head
    case None =>
  }

  (day, month)
}

def handleEqualSizedBlocks(groups: List[String]): (String, String, String) = {
  var year: String = "XXXX"
  var month: String = "XX"
  var day: String = "XX"

  groups.find { group =>
    group.toInt > 31
  } match {
    case Some(group) =>
      year = group
      val result = determineDateAndMonth(groups.filter(_ != group))
      month = result._1
      day = result._2
    case None =>
  }

  (year, month, day)
}

def padYearIfNeeded(year: String): String = {
  val currentYear: Int = 18 //TODO compute
  var paddedYear: String = year
  // The idea is kinda stupid because it won't allow dates which lie in the future
  // However, this leads to the problem that there will no way of determining the full year if only 2 digits are given
  if(year.length == 2 && year.toInt > currentYear) {
    paddedYear = "19" + year
  }

  paddedYear
}

def padSingleDigitDates(dates: List[String]): List[String] = {
  dates.map( date => if (date.length == 1 && date.forall(Character.isDigit)) "0" + date else date )
}

val splitPattern: Regex = "([0-9]+)([\\.\\-\\/\\s]{1})([0-9]+)([\\.\\-\\/]{1})([0-9]+)".r

for (date <- possible_dates) {
  for (patternMatch <- splitPattern.findFirstMatchIn(date)){
    var year: String = "XXXX"
    var month: String = "XX"
    var day: String = "XX"
    val groups = padSingleDigitDates(patternMatch.subgroups)
    val numGroups = List(groups.head, groups(2), groups(4))
    val separators = List(groups(1), groups(3))

    print("Numbers:", numGroups)
    println("Sep:", separators)

    numGroups.find { group =>
      group.length == 4
    } match {
      case Some(group) =>
        year = group
        val result = determineDateAndMonth(numGroups.filter(_ != group))
        day = result._1
        month = result._2
      case None =>
        val result = handleEqualSizedBlocks(numGroups)
        year = result._1
        month = result._2
        day = result._3
    }

    if (year != "XXXX" && month != "XX" && day != "XX") {
      val result = generatePatternAndRegex(groups, separators, year, month, day)
      println("Pattern: " + result._2 + " Regex: " + result._1)
      year = padYearIfNeeded(year)
    }

    println(s"YYYY-MM-DD: $year-$month-$day \n")
  }
}
