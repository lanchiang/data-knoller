package de.hpi.isg.dataprep.preparators

import de.hpi.isg.dataprep.DialectBuilder
import de.hpi.isg.dataprep.components.Pipeline
import de.hpi.isg.dataprep.context.DataContext
import de.hpi.isg.dataprep.load.FlatFileDataLoader
import de.hpi.isg.dataprep.model.target.system.AbstractPipeline
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}

trait PreparatorScalaTest extends FlatSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  var dataset: Dataset[Row] = _
  var pipeline: AbstractPipeline = _
  var dataContext: DataContext = _

  override def beforeAll: Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val filePath = getClass.getResource("/pokemon.csv").getPath
    val dialect = new DialectBuilder()
      .hasHeader(true)
      .inferSchema(true)
      .url(filePath)
      .buildDialect()
    val dataLoader = new FlatFileDataLoader(dialect)
    dataContext = dataLoader.load()
    //        dataContext.getDataFrame().show()
    super.beforeAll()
  }

  override def beforeEach: Unit = {
    pipeline = new Pipeline(dataContext)
    super.beforeEach()
  }
}
