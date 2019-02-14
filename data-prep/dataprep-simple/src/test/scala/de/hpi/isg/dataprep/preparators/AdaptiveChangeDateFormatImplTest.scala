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
class AdaptiveChangeDateFormatImplTest extends PreparatorScalaTest {

  override var testFileName = "dates.csv"

  "Dates" should "be formatted given a target format" in {
    val preparator = new AdaptiveChangeDateFormat("date", None, DatePattern.DatePatternEnum.DayMonthYear)

    val preparation: AbstractPreparation = new Preparation(preparator)
    pipeline.addPreparation(preparation)
    pipeline.executePipeline()

    val errorLogs: util.List[ErrorLog] = new util.ArrayList[ErrorLog]
    errorLogs.add(
      new PreparationErrorLog(preparation, "1989-01-00",
        new ParseException("No unambiguous pattern found to parse date. Date might be corrupted.", -1)))

    // Can't be parsed because this date was a Monday
    errorLogs.add(
      new PreparationErrorLog(preparation, "Tuesday, 13 Dec 1999",
        new ParseException("No unambiguous pattern found to parse date. Date might be corrupted.", -1)))

    val errorRepository: ErrorRepository = new ErrorRepository(errorLogs)

    println(pipeline.getErrorRepository.getPrintedReady)

    pipeline.getErrorRepository should equal(errorRepository)
  }
//
//  "Not enough evidence" should "return the type of text day and find the locale" in {
//    val impl = new DefaultAdaptiveChangeDateFormatImpl
//
//    val cluster1 = List(List("Sun","Aug", "2014"), List("Sun","Oct", "2009"), List("Mon","Sep", "2010"))
//
//    val foundTextDateField = impl.findValidPatternAndLocale(cluster1)
//    println(foundTextDateField)
//    foundTextDateField should equal(
//      Map(0 -> Some(impl.TextDateField("E", Locale.US)),
//        // PANAMA spanish
//        1 -> Some(impl.TextDateField("MMM", new Locale("es", "PA")))))
//  }
}