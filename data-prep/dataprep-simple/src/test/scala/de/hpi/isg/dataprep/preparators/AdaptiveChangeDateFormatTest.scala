package de.hpi.isg.dataprep.preparators

import java.util

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.metadata.PropertyDatePattern
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.preparators.define.AdaptiveChangeDateFormat
import de.hpi.isg.dataprep.selection.DataLoadingConfigScala
import de.hpi.isg.dataprep.util.DatePattern
import org.apache.spark.sql.functions.col

/**
  *
  * @author Hendrik Rätz, Nils Strelow
  * @since 2018/12/03
  */
class AdaptiveChangeDateFormatTest extends DataLoadingConfigScala {

  override var resourcePath: String = "/dates_applicability.csv"

  "calApplicability on the date column" should "return a score of 6/16" in {
    val columnName = "date"

    val metadata = new util.ArrayList[Metadata]()

    val preparator = new AdaptiveChangeDateFormat(columnName, None, DatePattern.DatePatternEnum.DayMonthYear)
    preparator.calApplicability(null, dataContext.getDataFrame.select(col(columnName)), metadata, null) should equal(5.0f/16.0f)
  }

  "calApplicability on the id column" should "return a score of 0" in {
    val columnName = "id"

    val metadata = new util.ArrayList[Metadata]()

    val preparator = new AdaptiveChangeDateFormat(columnName, None, DatePattern.DatePatternEnum.DayMonthYear)
    preparator.calApplicability(null, dataContext.getDataFrame.select(col(columnName)), metadata, null) should equal(0)
  }

  "calApplicability" should "return 0 for a column with metadata of a previous date formatting" in {

    val columnName = "date"

    val dateMetadata = new PropertyDatePattern(
      DatePattern.DatePatternEnum.DayMonthYear,
      new ColumnMetadata(columnName)
    )

    val metadata = new util.ArrayList[Metadata]()
    metadata.add(dateMetadata)

    val preparator = new AdaptiveChangeDateFormat(columnName, None, DatePattern.DatePatternEnum.DayMonthYear)
    preparator.buildMetadataSetup()

    preparator.calApplicability(null, dataContext.getDataFrame.select(col(columnName)), metadata, null) should equal(0)
  }
}