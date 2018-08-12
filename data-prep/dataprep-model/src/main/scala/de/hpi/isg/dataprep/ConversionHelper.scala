package de.hpi.isg.dataprep

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @author Lan Jiang
  * @since 2018/8/10
  */
@SerialVersionUID(1000L)
object ConversionHelper extends Serializable {

    def toDate(value: String, datePattern: String): Date = {
        val dateFormatter = new SimpleDateFormat(datePattern)
        dateFormatter.parse(value)
    }

    /**
      * Converts the pattern of the date value into the desired pattern.
      *
      * @param value the value to be converted
      * @param origin the origin date pattern specific by metadata
      * @param target the target date pattern.
      * @return the converted [[Date]]
      */
    def toDate(value: String, origin: String, target: String): Date = {
        new Date()
    }
}
