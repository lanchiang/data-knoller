package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.exceptions.ParameterNotSpecifiedException
import de.hpi.isg.dataprep.metadata.PropertyDatePattern
import de.hpi.isg.dataprep.model.target.objects.{ColumnMetadata, Metadata}
import de.hpi.isg.dataprep.preparators.define.ChangeDateFormat
import de.hpi.isg.dataprep.preparators.implementation.{DateRegex, DefaultChangeDateFormatImpl}
import de.hpi.isg.dataprep.util.DatePattern.DatePatternEnum

import scala.collection.JavaConverters._

class ChangeDateFormatTest extends PreparatorScalaTest {
  override var resourcePath: String = "/pokemon.csv"

  val pokemonDates = Set("11-12-1989", "12-11-1988", "24-02-1966", "05-02-1957", "08-08-2008", "07-01-2017")
  val pokemonDatesDifferentFormat = Set("12-11-1989")
  val pokemonInvalidDates = Set("thisIsDate", "2014-13-31", "2000-01-32")

  "AbstractPreparator" should "verify the pre execution conditions" in {
    val preparator = new ChangeDateFormat("date")
    val preparation = new Preparation(preparator)
    pipeline.addPreparation(preparation)
    an[ParameterNotSpecifiedException] should be thrownBy pipeline.executePipeline()
  }

  // this test is ignored since the required metadata (source pattern) is not automatically added to the metadata
  // repository and manually adding it is impossible since Pipeline::executePipeline() clears and rebuilds the
  // repository (and the rebuilt version does not contain the manually added entry)
  "Date format" should "be changed given source and target format" ignore {
    val preparator = ChangeDateFormat("date", DatePatternEnum.YearMonthDay, DatePatternEnum.DayMonthYear)
    val preparation = new Preparation(preparator)

    pipeline.addPreparation(preparation)
    pipeline.executePipeline()

    val parsedData = pipeline.getDataset
    parsedData.count() shouldBe 6

    val dates = parsedData.rdd
      .map(row => row.getString(row.fieldIndex("date")))
      .collect()
      .toSet
    dates shouldEqual pokemonDates

    val failedDates = pokemonInvalidDates ++ pokemonDatesDifferentFormat
    val expectedErrors = failedDates.map(date => s"""Unparseable date: "$date"""")
    val errors = pipeline
      .getErrorRepository
      .getErrorLogs
      .asScala
      .map(_.getErrorMessage)
    errors.toSet shouldEqual expectedErrors
  }

  "Date format" should "be changed given only the target format" in {
    val preparator = ChangeDateFormat("date", DatePatternEnum.DayMonthYear)
    val preparation = new Preparation(preparator)

    pipeline.addPreparation(preparation)
    pipeline.executePipeline()

    val parsedData = pipeline.getDataset
    parsedData.count() shouldBe 6

    val dates = parsedData.rdd
      .map(row => row.getAs[String]("date"))
      .collect()
      .toSet
    dates shouldEqual pokemonDates

    val failedDates = pokemonInvalidDates ++ pokemonDatesDifferentFormat
    val expectedErrors = failedDates.map(date => s"""Unparseable date: "$date"""")
    val errors = pipeline
      .getErrorRepository
      .getErrorLogs
      .asScala
      .map(_.getErrorMessage)
    errors.toSet shouldEqual expectedErrors
  }

  "Date format method" should "format date without source pattern" in {
    val implementation = new DefaultChangeDateFormatImpl
    val validDates = List(
      "11-03-2014",
      "1-03-2014",
      "1-3-2014",
      "14 10 1993",
      "1/3/1337"
    )
    val regex = DateRegex(DatePatternEnum.DayMonthYear.ordinal)
    val convertedDates = validDates.map(implementation.toDate(_, DatePatternEnum.DayMonthYear, regex))
    convertedDates.foreach { date => date should have length 10 }

    an[IllegalArgumentException] should be thrownBy implementation.toDate("1/1/1", DatePatternEnum.DayMonthYear, regex)
  }

  "Applicability score" should "be calculated" in {
    val preparator = ChangeDateFormat("date", DatePatternEnum.DayMonthYear)
    val pokemonData = pipeline.getDataset.select("date")
    val total = pokemonData.count()
    val score = preparator.calApplicability(null, pokemonData, Set().asJavaCollection)
    val expectedScore = (pokemonDates.size + pokemonDatesDifferentFormat.size).toFloat / total.toFloat
    score shouldEqual expectedScore
  }

  "Applicability score calculation" should "return 0.0 for multiple columns" in {
    val preparator = ChangeDateFormat("date", DatePatternEnum.DayMonthYear)
    val pokemonData = pipeline.getDataset
    val score = preparator.calApplicability(null, pokemonData, Set().asJavaCollection)
    score shouldEqual 0.0f
  }

  it should "return 0.0 if the preparator was already executed" in {
    val preparator = ChangeDateFormat("date", DatePatternEnum.DayMonthYear)
    val pokemonData = pipeline.getDataset.select("date")
    val metadata: Set[Metadata] = Set(new PropertyDatePattern(DatePatternEnum.DayMonthYear, new ColumnMetadata("date")))
    val score = preparator.calApplicability(null, pokemonData, metadata.asJavaCollection)
    score shouldEqual 0.0f
  }

  it should "return 0.0 if the input is not a string column" in {
    val preparator = ChangeDateFormat("date", DatePatternEnum.DayMonthYear)
    val pokemonData = pipeline.getDataset.select("height")
    val score = preparator.calApplicability(null, pokemonData, Set().asJavaCollection)
    score shouldEqual 0.0f
  }
}
