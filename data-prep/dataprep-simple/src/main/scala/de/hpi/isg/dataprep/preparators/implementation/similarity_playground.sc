import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import util.control.Breaks._

val dates: List[String] = List(
  "May 1st, 86",
  "Mar 30, 1998",
  "11 February 1939",
  "August 18",
  "May 1919",
  "July 03, 1800",
  "17 August",
  "18 August",
  "2013/14",
  "Aug 31, 2007",
  "1998-06-11")

def isDigit(s: String): Boolean = {
  s.forall(_.isDigit)
}

def isLetter(s: String): Boolean = {
  s.forall(_.isLetter)
}

def isAlphanumeric(s: String): Boolean = {
  s.forall(_.isLetterOrDigit)
}

def splitString(s: String): List[String] = {
  // Currently separators are split too so that ", " would become two different blocks: [,] [ ]
  // This is fine for now because our clustering is very strict
  val tmp: String = (s + 'X')
    .sliding(2)
    .map(pair => pair.head +
      (if (isAlphanumeric(pair.head.toString) && isAlphanumeric(pair.last.toString))
        ""
      else
        "&"))
    .mkString("")
  tmp.split("&").toList
}

println("Splitted dates")
for (date <- dates) {
  for (part <- splitString(date)) {
    print(s"$part&")
  }
  print("\n")
}

def getSimilarity(s1: String, s2: String): Double  = {
  val split1: List[String] = splitString(s1)
  val split2: List[String] = splitString(s2)
  var score: Double = 0.0
  val numberOfCriteria: Int = 3

  var idx: Int = 0
  var separatorsIdentical: Boolean = true
  var blockTypesIdentical: Boolean = true

  // iterate over blocks and check conditions for aligned blocks
  while (idx < split1.length && idx < split2.length) {
    val block1 = split1(idx)
    val block2 = split2(idx)


    if (!isAlphanumeric(block1) && !isAlphanumeric(block2)) {
      if (block1 != block2) {
        separatorsIdentical = false
      }
    } else {
      // checks if alphanumeric block at same position match type (number/letter)
      // TODO: check char for char because of dates which contain 1st/2nd
      if (!((isDigit(block1) && isDigit(block2)) || (isLetter(block1) && isLetter(block2)))) {
        blockTypesIdentical = false
      }
    }

    idx += 1
  }

  if (split1.length == split2.length) {
    score += 1
  }

  if (separatorsIdentical) {
    score += 1
  }

  if (blockTypesIdentical) {
    score += 1
  }

  score / numberOfCriteria
}

def clusterDates(dates: List[String]): mutable.Map[String, ListBuffer[List[String]]] = {
  val clusters: mutable.Map[String, ListBuffer[List[String]]] = mutable.Map()
  for (date <- dates) {
    breakable {
      for (clusterRep <- clusters.keys) {
        if (getSimilarity(clusterRep, date) == 1) {
          clusters(clusterRep).append(splitString(date))
          break
        }
      }
      clusters(date) = ListBuffer(splitString(date))
    }
  }

  clusters
}

val clusteredDates = clusterDates(dates)
for (key <- clusteredDates.keys) {
  println(s"$key, ${clusteredDates(key)}")
}
