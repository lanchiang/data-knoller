package de.hpi.isg.dataprep.selection

import de.hpi.isg.dataprep.preparators.define.DeleteProperty
import org.scalatest.{FlatSpecLike, Matchers}

class PreparatorLoaderTest  extends FlatSpecLike with Matchers {

  "The PreparatorLoader" should "load a preparator" in {
    class TestLoader extends PreparatorLoader {
      override val path: String =  "de.hpi.isg.preparators"
    }

    val x = new TestLoader
    x.preparators should contain (new DeleteProperty().getClass)
  }

}
