package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.components.Preparation
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.preparators.define.SplitProperty
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl
import de.hpi.isg.dataprep.preparators.implementation.DefaultSplitPropertyImpl.SingleValueStringSeparator
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

class SplitPropertyTest extends PreparatorScalaTest with Serializable {

  override var resourcePath: String = "/pokemon.csv"

  "Column" should "be split correctly if only propertyName is given" in {
    splitShouldBeCorrect(new SplitProperty("date"))
  }

  "Column" should "be split correctly if only propertyName and separator are given" in {
    splitShouldBeCorrect(new SplitProperty("date", SingleValueStringSeparator("-")))
  }

  "Column" should "be split correctly if all parameters are given" in {
    splitShouldBeCorrect(new SplitProperty("date", SingleValueStringSeparator("-")))
  }

  "Column" should "be split from the right" in {
    val separator = SingleValueStringSeparator("-")
    val preparator = new SplitProperty("date", separator, 3, false)
    pipeline.addPreparation(new Preparation(preparator))
    pipeline.executePipeline()

    pipeline.getErrorRepository.getErrorLogs.size shouldEqual 0
    pipeline.getDataset.schema shouldEqual newSchema

    pipeline.getDataset.collect().foreach(row => {
      val expectedSplit = row.get(8).toString.split("-").reverse
      val split = Array(row.get(12).toString, row.get(13).toString, row.get(14).toString)
      if (expectedSplit.length == 1)
        split shouldEqual Array(expectedSplit(0), "", "")
      else
        split shouldEqual expectedSplit
    })
  }

  "Applicability score" should "be calculated correctly" in {
    val separator = SingleValueStringSeparator("-")
    val impl = new DefaultSplitPropertyImpl()
    val column  = pipeline.getDataset.select("date").as(Encoders.STRING)
    val score = impl.evaluateSplit(column, separator, 3)
    score shouldEqual 0.9f

  }

  "IllegalArgumentException" should "be thrown if numCols is 1" in {
    val separator = SingleValueStringSeparator("-")
    val preparator = new SplitProperty("data", separator, 1, true)
    pipeline.addPreparation(new Preparation(preparator))
    an[IllegalArgumentException] should be thrownBy pipeline.executePipeline()
  }

  "IllegalArgumentException" should "be thrown if propertyName does not exist in dataset" in {
    val separator = SingleValueStringSeparator("-")
    val preparator = new SplitProperty("xyz", separator, 1, true)
    pipeline.addPreparation(new Preparation(preparator))
    an[IllegalArgumentException] should be thrownBy pipeline.executePipeline()
  }

  val newSchema = new StructType(
    Array[StructField](
      StructField("id", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("identifier", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("species_id", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("height", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("weight", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("base_experience", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("order", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("is_default", DataTypes.IntegerType, nullable = true, Metadata.empty),
      StructField("date", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("stemlemma", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("stemlemma2", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("stemlemma_wrong", DataTypes.StringType, nullable = true, Metadata.empty),
      StructField("date1", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("date2", DataTypes.StringType, nullable = false, Metadata.empty),
      StructField("date3", DataTypes.StringType, nullable = false, Metadata.empty)
    )
  )

  def splitShouldBeCorrect(preparator: AbstractPreparator): Unit = {
    pipeline.addPreparation(new Preparation(preparator))
    pipeline.executePipeline()

    pipeline.getErrorRepository.getErrorLogs.size shouldEqual 0
    pipeline.getDataset.schema shouldEqual newSchema

    pipeline.getDataset.collect().foreach(row => {
      val expectedSplit = row.get(8).toString.split("-")
      val split = Array(row.get(12).toString, row.get(13).toString, row.get(14).toString)
      if (expectedSplit.length == 1)
        split shouldEqual Array(expectedSplit(0), "", "")
      else
        split shouldEqual expectedSplit
    })
  }
}

