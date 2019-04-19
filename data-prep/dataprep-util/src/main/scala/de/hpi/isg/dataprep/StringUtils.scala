package de.hpi.isg.dataprep

/**
  * This class provides string processing utility functions.
  *
  * @author Lan Jiang
  * @since 2019-04-19
  */
object StringUtils {

  /**
    * calculates all possible substrings of the given string
    * @param str input string
    * @return list of substrings
    */
  def getAllSubstrings(str: String): Set[String] = {
    str.inits.flatMap(_.tails).toSet
  }

  /**
    * Calculates the longest common substring for two given strings.
    * @param str1 first string
    * @param str2	second string
    * @return longest substring
    */
  def longestCommonSubstring(str1: String, str2: String): String = {
    val str1Substrings = getAllSubstrings(str1)
    val str2Substrings = getAllSubstrings(str2)

    str1Substrings.intersect(str2Substrings).maxBy(_.length)
  }
}
