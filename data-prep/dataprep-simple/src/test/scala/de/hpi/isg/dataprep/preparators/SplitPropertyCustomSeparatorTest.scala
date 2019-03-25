package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.DialectBuilder
import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.io.load.{FlatFileDataLoader, SparkDataLoader}
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl.{MultiValueCharacterClassSeparator, MultiValueStringSeparator, SingleValueCharacterClassSeparator}
import de.hpi.isg.dataprep.preparators.implementation.SplitPropertyUtils
import de.hpi.isg.dataprep.selection.DataLoadingConfigScala
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Encoders

class SplitPropertyCustomSeparatorTest extends DataLoadingConfigScala with Serializable {

  override var resourcePath = "/split.csv"

  override protected def beforeAll(): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val filePath = getClass.getResource(resourcePath).toURI.getPath
    //    val path = File("src") / "test" / "resources" / "pokemon.csv" toString()
    dialect = new DialectBuilder()
            .hasHeader(true)
            .inferSchema(true)
            .url(filePath)
            .buildDialect

    val dataLoader: SparkDataLoader = new FlatFileDataLoader(dialect)
    dataContext = dataLoader.load
  }

  "MultiValueStringSeparator" should "work with single character as separator" in {
    val column = pipeline.getDataset.select("multi_string").as(Encoders.STRING)
    val distribution = SplitPropertyUtils.globalStringSeparatorDistribution(column, 2)
    val preparator = new SplitProperty(
      "multi_string",
      MultiValueStringSeparator(2, distribution),
      2
    )
    val expected = List(("a", "b"), ("a", "b"), ("a", "b"), ("a", "b"), ("a", "b"))
    splitShouldEqual(preparator, expected)
  }

  "MultiValueStringSeparator" should "work with multiple characters as separator" in {
    val column = pipeline.getDataset.select("multi_string2").as(Encoders.STRING)
    val distribution = SplitPropertyUtils.globalStringSeparatorDistribution(column, 2)
    val preparator = new SplitProperty(
      "multi_string2",
      MultiValueStringSeparator(2, distribution),
      2
    )
    val expected = List(("a", "b"), ("a", "b"), ("a", "b"), ("a", "b"), ("a", "b"))
    splitShouldEqual(preparator, expected)
  }

  "SingleValueCharacterClassSeparator" should "split multiple rows with the same character class" in {
    val column = pipeline.getDataset.select("single_char").as(Encoders.STRING)
    val preparator = new SplitProperty(
      "single_char",
      SingleValueCharacterClassSeparator("aA")
    )
    val expected = List(("Aaaa", "Bbbb"), ("Dddd", "Eeee"), ("Gggg", "Hhhh"), ("Jjjj", "Kkkk"), ("Mmmm", "Nnnn"))
    splitShouldEqual(preparator, expected)
  }

  "MultiValueCharacterClassSeparator" should "split multiple rows with different character classes" in {
    val column = pipeline.getDataset.select("multi_char").as(Encoders.STRING)
    val distribution = SplitPropertyUtils.globalTransitionSeparatorDistribution(column, 2)
    val preparator = new SplitProperty(
      "multi_char",
      MultiValueCharacterClassSeparator(2, distribution),
      2
    )
    val expected = List(("90", "$"), ("20", "a"), ("c", "30"), ("*", "11"), ("a", "A"))
    splitShouldEqual(preparator, expected)
  }

  def splitShouldEqual(preparator: SplitProperty, expectedResult: List[(String, String)]): Unit = {
    val spark = pipeline.getDataset.sparkSession
    import spark.implicits._

    pipeline.addPreparation(new Preparation(preparator))
    pipeline.executePipeline()
    pipeline.getErrorRepository.getErrorLogs.size shouldEqual 0

    val result = pipeline
      .getDataset.map(row => {
      (row.get(4).toString, row.get(5).toString)
    })
      .collect()
      .toList

    result shouldEqual expectedResult
  }
}

