package de.hpi.isg.dataprep.plot

import vegas._
import vegas.render.WindowRenderer._

/**
  * @author Lan Jiang
  * @since 2019-04-09
  */
object VegasTest {

  def main(args: Array[String]): Unit = {
    VegasTest.vegasPlot()
  }

  def vegasPlot(): Unit = {
    val plot = Vegas("Results").
            withData(
              Seq(
                Map("x" -> 1, "y" -> 1,"Origin"->"a"),
                Map("x" -> 2, "y" -> 2,"Origin"->"b"),
                Map("x" -> 3, "y" -> 3,"Origin"->"c")
              )
            ).
            encodeX("x", Quant).
            encodeY("y", Quant).
            encodeColor(field="Origin", dataType= Nominal).
            mark(Point)
    plot.show
  }
}
