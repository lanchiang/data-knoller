package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
class MergeAttribute (val attributes:List[String]
					 ,val connector:String)
		extends AbstractPreparator{

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

  def isDay(a: Dataset[Row], b: String): Boolean = {

    val sumDay = a.select(b).map(x => if (x.getInt(0) < 32 && x.getInt(0) > 0) 1 else 0)
      .sum

    //if (b.contains("day") || b.contains("Day")) true
    if (sumDay / a.count() == 1)
      true
    else
      false
  }


  override def buildMetadataSetup(): Unit = {

	}
	override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
		import dataset.sparkSession.implicits._
		val columns = dataset.columns.toSeq

		dataset.columns.flatMap (
			col => (
				col.zip(columns.filter(x => x != col))
				)
			)
				.map(x => dataset.select(x._1.toString, x._2))
				.map(colCombo =>
					colCombo.map(
						row => isGoodToMerge(row.getString(0), row.getString(1))
					)
							.reduce(_ + _)
				).max / dataset.count()

	}


}
