val dates: List[String] = List("Mar 30, 1998",
  "11 February 1939",
  "August 18",
  "May 1919",
  "17 August",
  "2013/14",
  "1998-06-11")

','.isLetter

def splitString(s: String): List[String] = {
  val tmp: String = (s + 'X')
    .sliding(2)
    .map(pair => pair.head +
      (if (pair.head.toString.forall(_.isDigit) && pair.last.toString.forall(_.isDigit)
        || (pair.head.toString.forall(_.isLetter) && pair.last.toString.forall(_.isLetter))
        || (pair.head.toString.forall(!_.isLetterOrDigit) && pair.last.toString.forall(!_.isLetterOrDigit))
      ) "" else "&"))
    .mkString("")
  tmp.split("&").toList
}

println("Splitted dates")
for (date <- dates) {
  for (part <- splitString(date)) {
    print(s"'$part', ")
  }
  print("\n")
}

//nach seperator, number/letter, length(4 nums = year), number of blocks
def getSimilarity(s1: String, s2: String): Double  = {
  val splitted1: List[String] = splitString(s1)
  val splitted2: List[String] = splitString(s2)
  var score: Double = 0.0

  if (splitted1.length == splitted2.length) {
    score += 1
  }

  score / 1
}

for (date1 <- dates) {
  for (date2 <- dates) {
    
  }
}
