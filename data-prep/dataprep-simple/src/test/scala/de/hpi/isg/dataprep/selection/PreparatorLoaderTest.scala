package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.preparators.define.DeleteProperty
import org.apache.log4j.{Level, Logger}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class PreparatorLoaderTest  extends FlatSpecLike with Matchers with BeforeAndAfterAll {

  override def beforeAll: Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    super.beforeAll()
  }

  "The PreparatorLoader" should "load a preparator" in {
    class TestLoader extends PreparatorLoader {
      override val path: String =  "de.hpi.isg.preparators"
    }

    val x = new TestLoader
    x.preparators should contain (new DeleteProperty().getClass)
  }

}
