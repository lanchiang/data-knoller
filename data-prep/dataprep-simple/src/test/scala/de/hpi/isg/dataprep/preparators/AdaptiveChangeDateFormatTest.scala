package de.hpi.isg.dataprep.preparators

import java.text.ParseException
import java.util
import java.util.Locale

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.metadata.PropertyDatePattern
import de.hpi.isg.dataprep.model.repository.ErrorRepository
import de.hpi.isg.dataprep.model.target.errorlog.{ErrorLog, PreparationErrorLog}
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.model.target.system.AbstractPreparation
import de.hpi.isg.dataprep.preparators.define.AdaptiveChangeDateFormat
import de.hpi.isg.dataprep.preparators.implementation.DefaultAdaptiveChangeDateFormatImpl
import de.hpi.isg.dataprep.util.DatePattern
import org.apache.spark.sql.functions.col

/**
  *
  * @author Hendrik Rätz, Nils Strelow
  * @since 2018/12/03
  */
class AdaptiveChangeDateFormatTest extends PreparatorScalaTest {

  override var testFileName = "dates_applicability.csv"

  "Dates" should "be formatted given a target format" in {
    val preparator = new AdaptiveChangeDateFormat("date", None, DatePattern.DatePatternEnum.DayMonthYear)

    val preparation: AbstractPreparation = new Preparation(preparator)
    pipeline.addPreparation(preparation)
    pipeline.executePipeline()

    val errorLogs: util.List[ErrorLog] = new util.ArrayList[ErrorLog]
    val errorLog: PreparationErrorLog =
      new PreparationErrorLog(preparation, "1989-01-00",
        new ParseException("No unambiguous pattern found to parse date. Date might be corrupted.", -1))
    errorLogs.add(errorLog)
    val errorRepository: ErrorRepository = new ErrorRepository(errorLogs)

    println(pipeline.getErrorRepository.getPrintedReady)

    pipeline.getErrorRepository should equal(errorRepository)
  }

  "calApplicability on the date column" should "return a score of 0.6" in {
    val columnName = "date"

    val metadata = new util.ArrayList[Metadata]()

    val preparator = new AdaptiveChangeDateFormat(columnName, None, DatePattern.DatePatternEnum.DayMonthYear)
    preparator.calApplicability(null, dataContext.getDataFrame.select(col(columnName)), metadata
    ) should equal(0.6.toFloat)
  }

  "calApplicability on the id column" should "return a score of 0" in {
    val columnName = "id"

    val metadata = new util.ArrayList[Metadata]()

    val preparator = new AdaptiveChangeDateFormat(columnName, None, DatePattern.DatePatternEnum.DayMonthYear)
    preparator.calApplicability(null, dataContext.getDataFrame.select(col(columnName)), metadata) should equal(0)
  }

  "calApplicability" should "return a score of 0 for a column with metadata of a previous date formatting" in {

    val columnName = "date"

    val dateMetadata = new PropertyDatePattern(
      DatePattern.DatePatternEnum.DayMonthYear,
      new ColumnMetadata(columnName)
    )

    val metadata = new util.ArrayList[Metadata]()
    metadata.add(dateMetadata)

    val preparator = new AdaptiveChangeDateFormat(columnName, None, DatePattern.DatePatternEnum.DayMonthYear)
    preparator.calApplicability(null, dataContext.getDataFrame.select(col(columnName)), metadata) should equal(0)
  }

  "Not enough evidence" should "return the type of text day and find the locale" in {
    val impl = new DefaultAdaptiveChangeDateFormatImpl

    val cluster1 = List(List("Sun","Aug", "2014"), List("Sun","Oct", "2009"), List("Mon","Sep", "2010"))

    val foundTextDateField = impl.findValidPatternAndLocale(cluster1)
    println(foundTextDateField)
    foundTextDateField should equal(
      Map(0 -> Some(impl.TextDateField("E", Locale.US)),
        // PANAMA spanish
        1 -> Some(impl.TextDateField("MMM", new Locale("es", "PA")))))
  }
}