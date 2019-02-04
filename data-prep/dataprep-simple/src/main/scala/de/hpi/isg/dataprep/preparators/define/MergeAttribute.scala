package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import org.apache.spark.sql.{Dataset, Row, functions}
import org.apache.spark.sql.functions.udf
import spire.std.float

import scala.collection.JavaConverters._
class MergeAttribute (val attributes:List[String]
					 ,val connector:String)
		extends AbstractPreparator{

	def this()
	{
		this(List[String](),"")
	}

	def this(attributes: java.util.List[String], connector:String)
	{
		this((attributes.asScala.toList), connector)
	}

	//def this(attributes: java.util.List[Integer], connector: String)
	//{
	//	this(Right(attributes.asScala.toList), connector)
	//}
	def isNull(value:String) = value.trim.isEmpty

	def isGoodToMerge(a:String, b:String):Integer = {
		if (a.equals(b))
			1
			else if ( isNull(a) || isNull(b))
			1
		else
			0
	}
	def mapMerge(row: Row) = isGoodToMerge(row.getString(0),row.getString(1))


  def isYear(a: Dataset[Row], b: String): Boolean = {

    val sumYears = a.select(b).map(x => if (x.getInt(0) < 2100 && x.getInt(0) > 0) 1 else 0)
      .sum

    //if (b.contains("year") || b.contains("Year")) true
    if (sumYears / a.count() == 1)
      true
    else
      false
  }

  def isMonth(a: Dataset[Row], b: String): Boolean = {

    val sumMonth = a.select(b).map(x => if (x.getInt(0) < 13 && x.getInt(0) > 0) 1 else 0)
      .sum

    //if (b.contains("month") || b.contains("Month")) true
    if (sumMonth / a.count() == 1)
      true
    else
      false
  }

  def isDay(row: Dataset[Row],header: String): Boolean = {
  import row.sparkSession.implicits._

    val columnWithoutNull = row
        .select(header)
        .filter(x => x.get(0).toString.trim.isEmpty)

    val sumDay = columnWithoutNull
        .map(x => if (x.getInt(0) <= 31 && x.getInt(0) > 0 ) 1 else 0).reduce(_+_)

    if (sumDay / columnWithoutNull.count() >= 0.95)
      true
    else
      false
  }


  override def buildMetadataSetup(): Unit = {

	}
	val weight = 4
	val bias = -0.75
	override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
		import dataset.sparkSession.implicits._
		val columns = dataset.columns.toSeq
		if (columns.length < 2) return 0

		val applicability = columns.flatMap (
			col => columns.map((col,_))
			).filter(x => x._1 != x._2)
				.map(x => dataset.select(x._1.toString, x._2))
        		.map(
				colCombo =>
					colCombo.map( x => {
						val a = x.get(0).toString
						val b = x.get(1).toString

						if (a.equals(b))
							1
						else if (a.trim.isEmpty || b.trim.isEmpty)
							1
						else
							0
					})
				)
       // 		.foreach(x => x.show())
        		.filter(x => x.count()>0)
				.map(colCombo =>
					colCombo
					.reduce(_ + _)
				).max / dataset.count().toFloat
		val result = (Math.max(applicability + bias,0) * weight).toFloat
		println(result)
		result
	}
}
