package de.hpi.isg.dataprep

import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum
import org.scalatest.{FlatSpec, Matchers}

class ConversionHelperTest extends FlatSpec with Matchers {
    "Date format" should "format date with source and target pattern" in {
        // TODO
    }

    it should "format date with out source pattern" in {
        val validDates = List(
            "11-03-2014",
            "1-03-2014",
            "1-3-2014",
            "1-3-14",
            "14 10 1993",
            "1 1 90",
            "1 10 80",
            "10 1 92",
            "1/3/37"
        )
        val convertedDates = validDates.map(ConversionHelper.toDate(_, DatePatternEnum.DayMonthYear))
        convertedDates.foreach { date => date should have length 10 }

        an [IllegalArgumentException] should be thrownBy ConversionHelper.toDate("1/1/1", DatePatternEnum.DayMonthYear)
    }
}