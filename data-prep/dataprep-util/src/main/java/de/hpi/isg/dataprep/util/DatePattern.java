package de.hpi.isg.dataprep.util;

import java.io.Serializable;

/**
 * This class describes the supported date pattern.
 *
 * @author Lan Jiang
 * @since 2018/8/13
 */
public class DatePattern implements Serializable {

    public final static DatePatternEnum[] SUPPORTED_DATE_PATTERN = {
            DatePatternEnum.DayMonthYear,
            DatePatternEnum.MonthDayYear,
            DatePatternEnum.YearDayMonth,
            DatePatternEnum.YearMonthDay
    };

    public enum DatePatternEnum {
        DayMonthYear("dd-MM-yyyy"),
        MonthDayYear("MM-dd-yyyy"),
        YearMonthDay("yyyy-MM-dd"),
        YearDayMonth("yyyy-dd-MM");

        private String pattern;

        DatePatternEnum(String pattern) {
            this.pattern = pattern;
        }

        public String getPattern() {
            return pattern;
        }
    }

}
