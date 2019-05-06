package de.hpi.isg.dataprep

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.util.{Failure, Success, Try}

/**
  * This class provides a variety of string processing utility functions.
  *
  * @author Lan Jiang
  * @since 2019-04-19
  */
object StringUtils {

  ////////////////////////////////////////////////////////////////////////////////////////////////
  // General methods                                                                            //
  ////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Returns `true` if the string contains only digits.
    *
    * @param s is the string to be checked
    * @return `true` if the string contains only digits
    */
  def isNumber(s: String): Boolean = {
    s.forall(_.isDigit)
  }

  /**
    * Returns `true` if the string contains only letters defined by [[Character]]'s isLetter method.
    *
    * @param s is the string to be checked
    * @return `true` if the string contains only letters
    */
  def isLetter(s: String): Boolean = {
    s.forall(_.isLetter)
  }

  /**
    * Returns `true` if the string contains only digits or letters.
    *
    * @param s is the string to be checked
    * @return `true` if the string contains only digits or letters.
    */
  def isAlphaNumeric(s: String): Boolean = {
    s.forall(_.isLetterOrDigit)
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  // Substring processing methods                                                               //
  ////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Returns all possible substrings of the given string
    *
    * @param str input string
    * @return list of substrings
    */
  def getAllSubstrings(str: String): Set[String] = {
    str.inits.flatMap(_.tails).toSet
  }

  /**
    * Returns the longest common substring for two given strings.
    *
    * @param str1 first string
    * @param str2	second string
    * @return longest substring
    */
  def longestCommonSubstring(str1: String, str2: String): String = {
    val str1Substrings = getAllSubstrings(str1)
    val str2Substrings = getAllSubstrings(str2)

    str1Substrings.intersect(str2Substrings).maxBy(_.length)
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  // Date string processing methods                                                             //
  ////////////////////////////////////////////////////////////////////////////////////////////////

  /**
    * Pads a single "0" to the string that represents a month or a day with only one digit.
    *
    * @param s the string to be padded
    * @return the padded string that represents the month or day with two digits.
    */
  def padSingleZeroToDateComponent(s: String): String = {
    s.length == 1 && s.forall(Character.isDigit) match {
      case true => "0" + s
      case false => s
    }
  }

  /**
    * Returns `true` if the given string can be parsed to the given date format.
    *
    * @param value the string to be parsed
    * @param sdf the given date format.
    * @return `true` if the given string can be parsed to the given date format.
    */
  def isParsableDateFormat(value: String, sdf: SimpleDateFormat): Boolean = {
    Try{
      sdf.parse(value)
    } match {
      case Failure(_) => false
      case Success(_) => true
    }
  }

  /**
    * Gets the string of the [[Date]] instance with the given date pattern and given locale.
    *
    * @param d the [[Date]] instance from which to get the date string
    * @param pattern the given date pattern
    * @param locale the given locale
    * @return the string of date that represents the given [[Date]] instance.
    */
  def getDateAsString(d: Date, pattern: String, locale: Locale): String = {
    val dateFormat = new SimpleDateFormat(pattern, locale)
    dateFormat.setLenient(false)
    dateFormat.format(d)
  }

  /**
    * Tries to convert the string given by `dateString` to the corresponding [[Date]] with the given date pattern
    * and the local. Returns an option of [[Date]].
    *
    * @param dateString the date string to be converted
    * @param datePattern the given date pattern
    * @param locale the given locale
    * @return an option of [[Date]]
    */
  def tryParseDate(dateString: String, datePattern: String, locale: Locale): Option[Date] = {
    Try{
      val dateFormat = new SimpleDateFormat(datePattern, locale)
      dateFormat.setLenient(false)
      dateFormat.parse(dateString)
    } match {
      case Failure(_) => None
      case Success(date) => Some(date)
    }
  }
}
