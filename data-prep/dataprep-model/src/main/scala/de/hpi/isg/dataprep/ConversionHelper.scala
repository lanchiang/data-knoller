package de.hpi.isg.dataprep

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import de.hpi.isg.dataprep.util.RemoveCharactersMode

import scala.util.{Failure, Success, Try}

/**
  * @author Lan Jiang
  * @since 2018/8/10
  */
@SerialVersionUID(1000L)
object ConversionHelper extends Serializable {

    private val defaultDateFormat = DatePatternEnum.YearMonthDay

    /**
      * Converts the pattern of the date value into the desired pattern.
      *
      * @param value  the value to be converted
      * @param source the origin date pattern specific by metadata
      * @param target the target date pattern.
      * @return the converted [[Date]]
      */
    @throws(classOf[Exception])
    def toDate(value: String, source: DatePatternEnum, target: DatePatternEnum): String = {
        /**
          * Succeeds to convert the date values in the correct origin format.
          * Cannot detect the incorrect order of y,m,d.
          */
        val sourceDatePattern = source.getPattern
        val sourceFormatter = new SimpleDateFormat(sourceDatePattern)
        sourceFormatter.setLenient(false)
        var sourceDateParse = Try{
            sourceFormatter.parse(value)
        }
        sourceDateParse match {
            case Failure(content) => {
                throw content
            }
            case date : Success[content] => {
                val targetDate = new SimpleDateFormat(target.getPattern).format(date.toOption.get)
                targetDate
            }
        }
    }

    def getDefaultDate(): String = {
        val defaultDate = new Date(0)
        val defaultFormatter = new SimpleDateFormat(defaultDateFormat.getPattern)
        defaultFormatter.format(defaultDate)
    }

    def getDefaultDateFormat(): String = {
        this.defaultDateFormat.getPattern
    }

    def collapse(value: String): String = {
        val newStr = value.replaceAll("\\s+", " ");
        newStr
    }

    def trim(value: String): String = {
        val newStr = value.trim
        newStr
    }

    def padding(value: String, expectedLength: Int, padder: String): String = {
        val len = value.length
        if (len > expectedLength) {
            throw new IllegalArgumentException(String.format("Value length is already larger than padded length."))
        }
        val paddingBitLen = expectedLength - len
        val padding = padder * paddingBitLen
        val newStr = padding + value
        newStr
    }

    def replaceSubstring(value: String, regex: String, replacement: String, firstSome: Int): String = {
        val processed = firstSome match {
            case count if count > 0 => {
                var newValue = new String(value)
                1 to count foreach {
                    _ => newValue = value.replaceFirst(regex, replacement)
                }
                newValue
            }
            case 0 => {
                value.replaceAll(regex, replacement)
            }
        }
        processed
    }

    def removeCharacters(value: String, mode: RemoveCharactersMode, custom: String) : String = {
        val changed = mode match {
            case RemoveCharactersMode.NUMERIC => {
                value.replaceAll("[0-9]+", "")
            }
            case RemoveCharactersMode.NONALPHANUMERIC => {
                value.replaceAll("([^0-9a-zA-Z])+", "")
            }
            case RemoveCharactersMode.CUSTOM => {
                value.replaceAll(custom, "")
            }
        }
        changed
    }
}
