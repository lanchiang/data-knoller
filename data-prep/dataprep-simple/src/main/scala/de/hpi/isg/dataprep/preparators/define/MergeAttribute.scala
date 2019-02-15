package de.hpi.isg.dataprep.preparators.define

import java.util

import de.hpi.isg.dataprep.model.target.objects.Metadata
import de.hpi.isg.dataprep.model.target.schema.SchemaMapping
import de.hpi.isg.dataprep.model.target.system.AbstractPreparator
import org.apache.spark.sql.{Dataset, Row, functions}
import org.apache.spark.sql.functions.udf
import spire.std.float

import scala.collection.JavaConverters._
class MergeAttribute (var attributes:List[String]
					 ,val connector:String)
		extends AbstractPreparator{
	var mergeDate = false
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


	def mapMerge(row: Row) = MergeUtil.isGoodToMerge(row.getString(0),row.getString(1))


  def headerIsYear(header: String): Boolean = if (header.contains("Year") || header.contains("year")) true else false

  def headerIsMonth(header: String): Boolean = if (header.contains("Month") || header.contains("month")) true else false

  def headerIsDay(header: String): Boolean = if (header.contains("Day") || header.contains("day")) true else false


  def isYear(row: Dataset[Row], header: String): Boolean = {
    import row.sparkSession.implicits._

    val columnWithoutNull = row
      .select(header)
      .filter(x => x.get(0).toString.trim.isEmpty)

    val sumYear = columnWithoutNull
      .map(x => if (x.getInt(0) <= 2100 && x.getInt(0) > 0 ) 1 else 0)
      .reduce(_+_)

    if (sumYear / columnWithoutNull.count() >= 1)
      true
    else
      false
  }

  def isMonth(row: Dataset[Row], header: String): Boolean = {
		import row.sparkSession.implicits._

		val columnWithoutNull = row
			.select(header)
			.filter(x => x.get(0).toString.trim.isEmpty)

		val sumMonth = columnWithoutNull
			.map(x => if (x.getInt(0) <= 12 && x.getInt(0) > 0 ) 1 else 0)
			.reduce(_+_)

    if (sumMonth / columnWithoutNull.count() >= 0.95)
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
			.map(x => if (x.getInt(0) <= 31 && x.getInt(0) > 0 ) 1 else 0)
			.reduce(_+_)

    if (sumDay / columnWithoutNull.count() >= 0.95)
      true
    else
      false
  }

//	def ksTest(dataset: Dataset[Row]):Double = {
//		dataset.sort("col1","col2")
//
//
//
//	}

  override def buildMetadataSetup(): Unit = {

	}

	val weight = 4
	val bias = -0.75

	def callApplicabilityDate(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]) :Float={
		import dataset.sparkSession.implicits._

    val dateCombi =  dataset.columns.seq.flatMap( x=>
      dataset.columns.map(y=>(y,x))
    ).filter(x=> x._1 != x._2)
      .flatMap( x=>
        dataset.columns.map(y=>(y,x._1,x._2))
      ).filter(x=> x._3 != x._2)
      .filter(x=> x._3 != x._1)
      .filter(x => headerIsDay(x._1) && headerIsMonth(x._2) && headerIsYear(x._3))


    if (dateCombi.seq.length>0){

      val result = dateCombi.seq(0)
      this.attributes = List(result._1,result._2,result._3)

      this.mergeDate = true

      1
    } else 0
	}

	override def calApplicability(schemaMapping: SchemaMapping, dataset: Dataset[Row], targetMetadata: util.Collection[Metadata]): Float = {
		import dataset.sparkSession.implicits._
		val columns = dataset.columns.toSeq
		if(columns.length == 3)
			return callApplicabilityDate(schemaMapping,dataset,targetMetadata)
		if (columns.length != 2)
			return 0

		val applicability = dataset
				.map( x => {
					val a = x.get(0).toString
					val b = x.get(1).toString
					MergeUtil.isGoodToMerge(a,b) //calculate mergeGoodness for each row
				})
				.reduce(_+_) / dataset.count().toFloat
		//apply threshold
		val result = (Math.max(applicability + bias,0) * weight).toFloat
		//remember columns for later merge operation
		this.attributes = columns.toList
		//return weighted mergeGoodness
		result
	}
}

object MergeUtil extends Serializable{
	def isNull(value:String) = value.trim.isEmpty
	def isGoodToMerge(a:String, b:String):Integer = {
		if (a.equals(b))
			1
		else if ( isNull(a) || isNull(b))
			1
		else
			0
	}
}
