package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl.MultiValueStringSeparator
import de.hpi.isg.dataprep.preparators.implementation.SplitPropertyUtils
import org.apache.spark.sql.Encoders

class SplitPropertyCustomSeparatorTest extends PreparatorScalaTest with Serializable {
  resourcePath = "/split.csv"

  "MultiValueStringSeparator" should "work with single character as separator" in {
    val column = pipeline.getRawData.select("multi_string").as(Encoders.STRING)
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
    val column = pipeline.getRawData.select("multi_string2").as(Encoders.STRING)
    val distribution = SplitPropertyUtils.globalStringSeparatorDistribution(column, 2)
    val preparator = new SplitProperty(
      "multi_string2",
      MultiValueStringSeparator(2, distribution),
      2
    )
    val expected = List(("a", "b"), ("a", "b"), ("a", "b"), ("a", "b"), ("a", "b"))
    splitShouldEqual(preparator, expected)
  }

  def splitShouldEqual(preparator: SplitProperty, expectedResult: List[(String, String)]): Unit = {
    val spark = pipeline.getRawData.sparkSession
    import spark.implicits._

    pipeline.addPreparation(new Preparation(preparator))
    pipeline.executePipeline()
    pipeline.getErrorRepository.getErrorLogs.size shouldEqual 0

    val result = pipeline
      .getRawData.map(row => {
      (row.get(4).toString, row.get(5).toString)
    })
      .collect()
      .toList

    result shouldEqual expectedResult
  }
}

