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
		this(attributes.asScala.toList,connector)
	}


	override def buildMetadataSetup(): Unit = {

	}
	override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = 0


}
