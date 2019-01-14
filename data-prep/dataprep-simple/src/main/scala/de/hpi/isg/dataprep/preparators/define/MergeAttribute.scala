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
