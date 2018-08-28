package de.hpi.isg.dataprep

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum

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
}
