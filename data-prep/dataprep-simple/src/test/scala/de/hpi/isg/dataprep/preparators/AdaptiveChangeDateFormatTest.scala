package de.hpi.isg.dataprep.preparators

import java.text.ParseException
import java.util

import de.hpi.isg.dataprep.DialectBuilder
import de.hpi.isg.dataprep.components.{Pipeline, Preparation, Preparator}
import de.hpi.isg.dataprep.context.DataContext
import de.hpi.isg.dataprep.load.FlatFileDataLoader
import de.hpi.isg.dataprep.model.repository.ErrorRepository
import de.hpi.isg.dataprep.model.target.errorlog.{ErrorLog, PreparationErrorLog}
import de.hpi.isg.dataprep.model.target.system.{AbstractPipeline, AbstractPreparation}
import de.hpi.isg.dataprep.preparators.define.AdaptiveChangeDateFormat
import de.hpi.isg.dataprep.util.DatePattern
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row}
import org.junit.Assert
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  *
  * @author Hendrik Rätz, Nils Strelow
  * @since 2018/12/03
  */
class AdaptiveChangeDateFormatTest extends FunSuite with BeforeAndAfter {

  protected var dataset: Dataset[Row] = null
  protected var pipeline: AbstractPipeline = null
  protected var dataContext: DataContext = null

  before {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val dialect = new DialectBuilder().hasHeader(true).inferSchema(true).url("../dataprep-simple/src/test/resources/dates.csv").buildDialect
    val dataLoader = new FlatFileDataLoader(dialect)
    dataContext = dataLoader.load
    pipeline = new Pipeline(dataContext)
  }

  test("AdaptiveChangeDateFormatTest.execute") {
    val preparator: Preparator = new AdaptiveChangeDateFormat("date", None, DatePattern.DatePatternEnum.DayMonthYear)

    val preparation: AbstractPreparation = new Preparation(preparator)
    pipeline.addPreparation(preparation)
    pipeline.executePipeline()

    val errorLogs: util.List[ErrorLog] = new util.ArrayList[ErrorLog]
    val errorLog: PreparationErrorLog = new PreparationErrorLog(preparation, "1989-01-00", new ParseException("No unambiguous pattern found to parse date. Date might be corrupted.", -1))
    errorLogs.add(errorLog)
    val errorRepository: ErrorRepository = new ErrorRepository(errorLogs)

    pipeline.getRawData.show()

    println(pipeline.getErrorRepository.getPrintedReady)

    Assert.assertEquals(errorRepository, pipeline.getErrorRepository)
  }
}