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
  "31.01.16",
  "25.1.16",
  "9.1.16",
  "04/28/1996",
  "09/17/96",
  "1975-01-18",
  "75-01-18",
  "75 01 18",
  "20020330",
  "29 Jul. 1935"
)

val splitPattern: Regex = "([0-9]+)([\\.\\-\\/\\s]{1})([0-9]+)([\\.\\-\\/]{1})([0-9]+)".r

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

def generatePlaceholder(origString:String, placeholder:String):String = {
  origString.replaceAll(".", placeholder)
}


def generatePatternAndRegex(fullGroup:List[String], separators:List[String], year: String, month: String, day: String): (String, String) = {
  var newGroup = fullGroup.patch(fullGroup.indexOf(year), generatePlaceholder(year, "Y"), 1).asInstanceOf[List[String]]
  newGroup = newGroup.patch(newGroup.indexOf(month), generatePlaceholder(month, "M"), 1).asInstanceOf[List[String]]
  newGroup = newGroup.patch(newGroup.indexOf(day), generatePlaceholder(day, "D"), 1).asInstanceOf[List[String]]

  var pattern: String = ""
  var regex: String = ""

  for(groupAsString <- newGroup) {
    pattern = pattern + groupAsString

    if (groupAsString.matches("[0-9]+")) {
      regex = regex + s"[0-9]{${groupAsString.length}}" // TODO: this leads to different regexes 1.1.2018 and 10.10.2018 although they are the same
    } else {
      regex = regex + groupAsString
    }
  }

  (regex, pattern)
}

for (date <- possible_dates) {
  for (patternMatch <- splitPattern.findFirstMatchIn(date)){
    var year: String = "XXXX"
    var month: String = "XX"
    var day: String = "XX"
    val groups = patternMatch.subgroups
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
      println("Pattern: " + result._2)
      year = padYearIfNeeded(year)
    }

    println(s"YYYY-MM-DD: $year-$month-$day \n")
  }
}

