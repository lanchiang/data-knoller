package de.hpi.isg.dataprep.experiment

import de.hpi.isg.dataprep.components.DecisionEngine
import de.hpi.isg.dataprep.experiment.define.MergePropertySuggest
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import de.hpi.isg.dataprep.utils.SimplePipelineBuilder
import org.apache.spark.sql.SparkSession
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

/**
  * @author Lan Jiang
  * @since 2019-04-17
  */
class MergePropertySuggestTest extends FlatSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

//  var sparkContext: SparkSession = _
//
//  override protected def beforeAll(): Unit = {
//    val sparkBuilder = SparkSession
//            .builder()
//            .appName("MergePropertySuggestTest")
//            .master("local[4]")
//    sparkContext = sparkBuilder.getOrCreate()
//    super.beforeAll()
//  }

  "The decision engine" should "suggest a merge property preparator" in {
    val resourcePath = getClass.getResource("/merge-property/interleaving_string.csv").toURI.getPath
    val pipeline = SimplePipelineBuilder.fromParameterBuilder(resourcePath)

    val decisionEngine = DecisionEngine.getInstance()
    decisionEngine.setPreparatorCandidates(Array("MergePropertySuggest"))

    val actualPreparator = decisionEngine.selectBestPreparators(pipeline)
    val expectedPreparator = new MergePropertySuggest(Array("_c0", "_c1").toList, null)

    actualPreparator shouldBe a [MergePropertySuggest]
    val casted = actualPreparator.asInstanceOf[MergePropertySuggest]
    Assert.assertEquals(expectedPreparator.attributes, casted.attributes)
    Assert.assertEquals(expectedPreparator.connector, casted.connector)
  }
}
