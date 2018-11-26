import scala.util.matching.Regex

// Concept:
// Separators are either "/",".","-" or no separator at all
// Always 3 groups of numbers which are either 1/2 digits long (day/month) or 2/4 (year)
// Year either in front or at the back otherwise stupid
// Additional stuff:
// Some months have less days
// Months could be represented as Stings, such as "Jan", "Oct"

// Stuff:
//val yyyy_mm_dd_pattern: Regex = "(([12]\\d{3})-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01]))".r
//val dd_mm_yyyy_pattern: Regex = "((0[1-9]|[12]\\d|3[01])-(0[1-9]|1[0-2])-([12]\\d{3}))".r
//
//yyyy_mm_dd_pattern.findFirstMatchIn("1999-12-11") match {
//  case Some(_) => println("Found")
//  case None => println("No pattern found")
//}

val possible_dates = Array(
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

val splitPattern: Regex = "([0-9]+)[\\.\\-\\/\\s]{1}([0-9]+)[\\.\\-\\/]([0-9]+)".r

for (date <- possible_dates) {
  for (patternMatch <- splitPattern.findFirstMatchIn(date)){
    var year: String = ""
    var month: String = ""
    var day: String = ""

    val first: String = patternMatch.group(1)
    val second: String = patternMatch.group(2)
    val third: String = patternMatch.group(3)

    println(
      s"First: $first, " +
      s"Second: $second, " +
      s"Third: $third"
    )

    if(first.length == 4) {
      year = first
      val result = determineDateAndMonth(second, third)
      day = result._1
      month = result._2
    } else if (third.length == 4) {
      year = third
      val result = determineDateAndMonth(first, second)
      day = result._1
      month = result._2
    } else {
      val result = handleEqualSizedBlocks(first, second, third)
      year = result._1
      month = result._2
      day = result._3
    }
    println(s"$year, $month, $day \n")
  }
}

def determineDateAndMonth(first: String, second: String): (String, String) = {
  var day: String = "-1"
  var month: String = "-1"

  if (first.toInt > 12) {
    month = first
    day = second
  } else if (second.toInt > 12) {
    day = first
    month = second
  }

  (day, month)
}

def handleEqualSizedBlocks(first: String, second: String, third: String): (String, String, String) = {
  var year: String = "-1"
  var month: String = "-1"
  var day: String = "-1"



  (year, month, day)
}

